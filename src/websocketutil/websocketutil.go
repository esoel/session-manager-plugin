// Modified by the ssmc project: cancellable WebSocket setup and HTTP upgrade.
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

// Package websocketutil contains methods for interacting with websocket connections.
package websocketutil

import (
	"context"
	"errors"
	"net"

	"github.com/esoel/session-manager-plugin/src/log"
	"github.com/gorilla/websocket"
)

// IWebsocketUtil is the interface for the websocketutil.
type IWebsocketUtil interface {
	OpenConnection(url string) (*websocket.Conn, error)
	CloseConnection(ws websocket.Conn) error
}

// WebsocketUtil struct provides functionality around creating and maintaining websockets.
type WebsocketUtil struct {
	dialer *websocket.Dialer
	log    log.T
}

// NewWebsocketUtil is the factory function for websocketutil.
func NewWebsocketUtil(logger log.T, dialerInput *websocket.Dialer) *WebsocketUtil {

	var websocketUtil *WebsocketUtil

	if dialerInput == nil {
		websocketUtil = &WebsocketUtil{
			dialer: websocket.DefaultDialer,
			log:    logger,
		}
	} else {
		websocketUtil = &WebsocketUtil{
			dialer: dialerInput,
			log:    logger,
		}
	}

	return websocketUtil
}

// OpenConnection opens a websocket connection provided an input url.
func (u *WebsocketUtil) OpenConnection(url string) (*websocket.Conn, error) {
	return u.OpenConnectionContext(context.Background(), url)
}

// OpenConnectionContext cancels both dialing and the HTTP upgrade handshake.
func (u *WebsocketUtil) OpenConnectionContext(ctx context.Context, url string) (*websocket.Conn, error) {

	u.log.Infof("Opening websocket connection to: ", url)

	// Gorilla's DialContext uses the context for dialing, but an HTTP upgrade
	// already in progress is bounded only by its deadline. Close that socket
	// explicitly on cancellation as well.
	dialer := *u.dialer
	netDial := dialer.NetDialContext
	if netDial == nil {
		if dialer.NetDial != nil {
			netDial = func(_ context.Context, network, addr string) (net.Conn, error) { return dialer.NetDial(network, addr) }
		} else {
			netDial = (&net.Dialer{}).DialContext
		}
	}
	var stopCancel func() bool
	dialer.NetDialContext = func(dialCtx context.Context, network, addr string) (net.Conn, error) {
		socket, err := netDial(dialCtx, network, addr)
		if err == nil {
			stopCancel = context.AfterFunc(ctx, func() { socket.Close() })
		}
		return socket, err
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if stopCancel != nil {
		stopCancel()
	}
	if ctx.Err() != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, ctx.Err()
	}
	if err != nil {
		u.log.Errorf("Failed to dial websocket: %s", err.Error())
		return nil, err
	}

	u.log.Infof("Successfully opened websocket connection to: ", url)

	return conn, err
}

// CloseConnection closes a websocket connection given the Conn object as input.
func (u *WebsocketUtil) CloseConnection(ws *websocket.Conn) error {

	if ws == nil {
		return errors.New("websocket conn object is nil")
	}

	u.log.Debugf("Closing websocket connection to:", ws.RemoteAddr().String())

	err := ws.Close()
	if err != nil {
		u.log.Errorf("Failed to close websocket: %s", err.Error())
		return err
	}

	u.log.Debugf("Successfully closed websocket connection to:", ws.RemoteAddr().String())

	return nil
}
