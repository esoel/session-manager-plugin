// Modified by the ssmc project: embedding lifecycle and listener hooks.
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

// Package session starts the session.
package session

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"

	sdkSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/esoel/session-manager-plugin/src/config"
	"github.com/esoel/session-manager-plugin/src/log"
	"github.com/esoel/session-manager-plugin/src/message"
	"github.com/esoel/session-manager-plugin/src/retry"
	"github.com/esoel/session-manager-plugin/src/sdkutil"
)

// OpenDataChannel initializes datachannel
func (s *Session) OpenDataChannel(log log.T) (err error) {
	s.retryParams = retry.RepeatableExponentialRetryer{
		GeometricRatio:      config.RetryBase,
		InitialDelayInMilli: rand.Intn(config.DataChannelRetryInitialDelayMillis) + config.DataChannelRetryInitialDelayMillis,
		MaxDelayInMilli:     config.DataChannelRetryMaxIntervalMillis,
		MaxAttempts:         config.DataChannelNumMaxRetries,
	}

	s.DataChannel.Initialize(log, s.ClientId, s.SessionId, s.TargetId, s.IsAwsCliUpgradeNeeded)
	if channel, ok := s.DataChannel.(interface{ SetContext(context.Context) }); ok {
		channel.SetContext(s.Context())
	}
	s.DataChannel.SetWebsocket(log, s.StreamUrl, s.TokenValue)
	s.DataChannel.GetWsChannel().SetOnMessage(
		func(input []byte) {
			s.DataChannel.OutputMessageHandler(log, s.Stop, s.SessionId, input)
		})
	s.DataChannel.RegisterOutputStreamHandler(s.ProcessFirstMessage, false)

	// Serialize the initial open and error callbacks. A peer can disconnect
	// during the initial token handshake, before Open returns.
	var connectionMu sync.Mutex
	s.DataChannel.GetWsChannel().SetOnError(
		func(err error) {
			connectionMu.Lock()
			defer connectionMu.Unlock()
			if s.Context().Err() != nil {
				return
			}
			log.Errorf("Trying to reconnect session %s", s.SessionId)
			retries := s.retryParams
			retries.CallableFunc = func() error { return s.ResumeSessionHandler(log) }
			if err = retries.CallContext(s.Context()); err != nil {
				log.Error(err)
				s.Stop()
			}
		})

	connectionMu.Lock()
	err = s.DataChannel.Open(log)
	if err != nil {
		retries := s.retryParams
		retries.CallableFunc = func() error { return s.DataChannel.Reconnect(log) }
		err = retries.CallContext(s.Context())
	}
	connectionMu.Unlock()
	if err != nil {
		return err
	}

	// Scheduler for resending of data
	s.DataChannel.ResendStreamDataMessageScheduler(log)

	return nil
}

// ProcessFirstMessage only processes messages with PayloadType Output to determine the
// sessionType of the session to be launched. This is a fallback for agent versions that do not support handshake, they
// immediately start sending shell output.
func (s *Session) ProcessFirstMessage(log log.T, outputMessage message.ClientMessage) (isHandlerReady bool, err error) {
	// Immediately deregister self so that this handler is only called once, for the first message
	s.DataChannel.DeregisterOutputStreamHandler(s.ProcessFirstMessage)
	// Only set session type if the session type has not already been set. Usually session type will be set
	// by handshake protocol which would be the first message but older agents may not perform handshake
	if s.SessionType == "" {
		if outputMessage.PayloadType == uint32(message.Output) {
			log.Warn("Setting session type to shell based on PayloadType!")
			s.DataChannel.SetSessionType(config.ShellPluginName)
			s.DisplayMode.DisplayMessage(log, outputMessage)
		}
	}
	return true, nil
}

// StopHook lets an embedding application handle session termination.
// Set once before starting sessions; the session ID identifies the owner.
var StopHook func(string)

// Stop will end the session
func (s *Session) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if StopHook != nil {
		StopHook(s.SessionId)
		return
	}
	os.Exit(0)
}

// GetResumeSessionParams calls ResumeSession API and gets tokenvalue for reconnecting
func (s *Session) GetResumeSessionParams(log log.T) (string, error) {
	var (
		resumeSessionOutput *ssm.ResumeSessionOutput
		err                 error
		sdkSession          *sdkSession.Session
	)

	if sdkSession, err = sdkutil.GetNewSessionWithEndpoint(s.Endpoint); err != nil {
		return "", err
	}
	s.sdk = ssm.New(sdkSession)

	resumeSessionInput := ssm.ResumeSessionInput{
		SessionId: &s.SessionId,
	}

	log.Debugf("Resume Session input parameters: %v", resumeSessionInput)
	if resumeSessionOutput, err = s.sdk.ResumeSessionWithContext(s.Context(), &resumeSessionInput); err != nil {
		log.Errorf("Resume Session failed: %v", err)
		return "", err
	}

	if resumeSessionOutput.TokenValue == nil {
		return "", nil
	}

	return *resumeSessionOutput.TokenValue, nil
}

// ResumeSessionHandler gets token value and tries to Reconnect to datachannel
func (s *Session) ResumeSessionHandler(log log.T) (err error) {
	s.TokenValue, err = s.GetResumeSessionParams(log)
	if err != nil {
		log.Errorf("Failed to get token: %v", err)
		return
	} else if s.TokenValue == "" {
		log.Debugf("Session: %s timed out", s.SessionId)
		fmt.Fprintf(os.Stdout, "Session: %s timed out.\n", s.SessionId)
		if StopHook != nil {
			s.Stop()
			return fmt.Errorf("session timed out")
		}
		os.Exit(0)
	}
	s.DataChannel.GetWsChannel().SetChannelToken(s.TokenValue)
	err = s.DataChannel.Reconnect(log)
	return
}

// TerminateSession calls TerminateSession API
func (s *Session) TerminateSession(log log.T) error {
	var (
		err        error
		newSession *sdkSession.Session
	)

	if newSession, err = sdkutil.GetNewSessionWithEndpoint(s.Endpoint); err != nil {
		log.Errorf("Terminate Session failed: %v", err)
		return err
	}
	s.sdk = ssm.New(newSession)

	terminateSessionInput := ssm.TerminateSessionInput{
		SessionId: &s.SessionId,
	}

	log.Debugf("Terminate Session input parameters: %v", terminateSessionInput)
	if _, err = s.sdk.TerminateSessionWithContext(s.Context(), &terminateSessionInput); err != nil {
		log.Errorf("Terminate Session failed: %v", err)
		return err
	}
	return nil
}
