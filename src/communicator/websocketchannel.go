// Modified by the ssmc project: cancellable, joined transport workers.
// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may not
// use this file except in compliance with the License. A copy of the
// License is located at
//
// http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing
// permissions and limitations under the License.

// this package implement base communicator for network connections.
package communicator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/esoel/session-manager-plugin/src/config"
	"github.com/esoel/session-manager-plugin/src/log"
	"github.com/esoel/session-manager-plugin/src/websocketutil"
	"github.com/gorilla/websocket"
)

// IWebSocketChannel is the interface for DataChannel.
type IWebSocketChannel interface {
	Initialize(log log.T, channelUrl string, channelToken string)
	Open(log log.T) error
	Close(log log.T) error
	SendMessage(log log.T, input []byte, inputType int) error
	StartPings(log log.T, pingInterval time.Duration)
	GetChannelToken() string
	GetStreamUrl() string
	SetChannelToken(string)
	SetOnError(onErrorHandler func(error))
	SetOnMessage(onMessageHandler func([]byte))
}

// WebSocketChannel parent class for DataChannel.
type WebSocketChannel struct {
	IWebSocketChannel
	ctx          context.Context
	stateMu      sync.Mutex
	workers      sync.WaitGroup
	done         chan struct{}
	Url          string
	OnMessage    func([]byte)
	OnError      func(error)
	IsOpen       bool
	writeLock    sync.Mutex
	Connection   *websocket.Conn
	ChannelToken string
}

// GetChannelToken gets the channel token
func (webSocketChannel *WebSocketChannel) GetChannelToken() string {
	return webSocketChannel.ChannelToken
}

// SetChannelToken sets the channel token
func (webSocketChannel *WebSocketChannel) SetChannelToken(channelToken string) {
	webSocketChannel.ChannelToken = channelToken
}

// GetStreamUrl gets stream url
func (webSocketChannel *WebSocketChannel) GetStreamUrl() string {
	return webSocketChannel.Url
}

// SetOnError sets OnError field of websocket channel
func (webSocketChannel *WebSocketChannel) SetOnError(onErrorHandler func(error)) {
	webSocketChannel.stateMu.Lock()
	defer webSocketChannel.stateMu.Unlock()
	webSocketChannel.OnError = onErrorHandler
}

// SetOnMessage sets OnMessage field of websocket channel
func (webSocketChannel *WebSocketChannel) SetOnMessage(onMessageHandler func([]byte)) {
	webSocketChannel.stateMu.Lock()
	defer webSocketChannel.stateMu.Unlock()
	webSocketChannel.OnMessage = onMessageHandler
}

// SetContext must be called before Open. Cancellation closes the active socket.
func (webSocketChannel *WebSocketChannel) SetContext(ctx context.Context) {
	webSocketChannel.ctx = ctx
}

// Wait joins all transport workers, including a pending reconnect callback.
// Call after cancelling the session and closing its connection, outside callbacks.
func (webSocketChannel *WebSocketChannel) Wait() { webSocketChannel.workers.Wait() }

// Initialize initializes websocket channel fields
func (webSocketChannel *WebSocketChannel) Initialize(log log.T, channelUrl string, channelToken string) {
	webSocketChannel.ChannelToken = channelToken
	webSocketChannel.Url = channelUrl
}

// StartPings starts the pinging process to keep the websocket channel alive.
func (webSocketChannel *WebSocketChannel) StartPings(log log.T, pingInterval time.Duration) {
	webSocketChannel.stateMu.Lock()
	conn, done := webSocketChannel.Connection, webSocketChannel.done
	if !webSocketChannel.IsOpen {
		webSocketChannel.stateMu.Unlock()
		return
	}
	webSocketChannel.workers.Add(1)
	webSocketChannel.stateMu.Unlock()
	go func() {
		defer webSocketChannel.workers.Done()
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			err := conn.WriteControl(websocket.PingMessage, []byte("keepalive"), time.Now().Add(10*time.Second))
			if err != nil {
				log.Errorf("Error while sending websocket ping: %v", err)
				return
			}
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
}

// SendMessage sends a byte message through the websocket connection.
// Examples of message type are websocket.TextMessage or websocket.Binary
func (webSocketChannel *WebSocketChannel) SendMessage(log log.T, input []byte, inputType int) error {
	webSocketChannel.stateMu.Lock()
	conn, open := webSocketChannel.Connection, webSocketChannel.IsOpen
	webSocketChannel.stateMu.Unlock()
	if !open {
		return errors.New("Can't send message: Connection is closed.")
	}

	if len(input) < 1 {
		return errors.New("Can't send message: Empty input.")
	}

	webSocketChannel.writeLock.Lock()
	err := conn.WriteMessage(inputType, input)
	webSocketChannel.writeLock.Unlock()
	return err
}

// Close closes the corresponding connection.
func (webSocketChannel *WebSocketChannel) Close(log log.T) error {
	webSocketChannel.stateMu.Lock()
	defer webSocketChannel.stateMu.Unlock()
	if webSocketChannel.IsOpen {
		webSocketChannel.IsOpen = false
		close(webSocketChannel.done)
		return webSocketChannel.Connection.Close()
	}
	return nil
}

// Open upgrades the http connection to a websocket connection.
func (webSocketChannel *WebSocketChannel) Open(log log.T) error {
	ctx := webSocketChannel.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ws, err := websocketutil.NewWebsocketUtil(log, nil).OpenConnectionContext(ctx, webSocketChannel.Url)
	if err != nil {
		return err
	}
	webSocketChannel.stateMu.Lock()
	webSocketChannel.Connection = ws
	webSocketChannel.IsOpen = true
	done := make(chan struct{})
	webSocketChannel.done = done
	webSocketChannel.workers.Add(2)
	webSocketChannel.stateMu.Unlock()
	webSocketChannel.StartPings(log, config.PingTimeInterval)
	go func() {
		defer webSocketChannel.workers.Done()
		select {
		case <-ctx.Done():
			ws.Close()
		case <-done:
		}
	}()

	// spin up a different routine to listen to the incoming traffic
	go func() {
		defer webSocketChannel.workers.Done()
		defer func() {
			if msg := recover(); msg != nil {
				log.Errorf("WebsocketChannel listener run panic: %v", msg)
			}
		}()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:
			}
			messageType, rawMessage, err := ws.ReadMessage()
			webSocketChannel.stateMu.Lock()
			onError, onMessage := webSocketChannel.OnError, webSocketChannel.OnMessage
			webSocketChannel.stateMu.Unlock()
			if err != nil {
				// A WebSocket read failure is permanent for this connection.
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				default:
				}
				if onError != nil {
					onError(err)
				}
				return
			} else if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
				// We only accept text messages which are interpreted as UTF-8 or binary encoded text.
				log.Errorf("Invalid message type. We only accept UTF-8 or binary encoded text. Message type: %v", messageType)

			} else {
				if onMessage != nil {
					onMessage(rawMessage)
				}
			}
		}
	}()
	return nil
}
