// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/util/random"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type QRChannelItem struct {
	// The type of event, "code" for new QR codes (see Code field) and "error" for pairing errors (see Error) field.
	// For non-code/error events, you can just compare the whole item to the event variables (like QRChannelSuccess).
	Event string
	// If the item is a pair error, then this field contains the error message.
	Error error
	// If the item is a new code, then this field contains the raw data.
	Code string
	// The timeout after which the next code will be sent down the channel.
	Timeout time.Duration

	PasskeyRequest      *events.PairPasskeyRequest
	PasskeyConfirmation *events.PairPasskeyConfirmation
}

const QRChannelEventCode = "code"
const QRChannelEventError = "error"
const QRChannelEventPasskeyRequest = "passkey-request"
const QRChannelEventPasskeyResponse = "passkey-confirmation"

// Possible final items in the QR channel. In addition to these, an `error` event may be emitted,
// in which case the Error field will have the error that occurred during pairing.
var (
	// QRChannelSuccess is emitted from GetQRChannel when the pairing is successful.
	QRChannelSuccess = QRChannelItem{Event: "success"}
	// QRChannelTimeout is emitted from GetQRChannel if the socket gets disconnected by the server before the pairing is successful.
	QRChannelTimeout = QRChannelItem{Event: "timeout"}
	// QRChannelErrUnexpectedEvent is emitted from GetQRChannel if an unexpected connection event is received,
	// as that likely means that the pairing has already happened before the channel was set up.
	QRChannelErrUnexpectedEvent = QRChannelItem{Event: "err-unexpected-state"}
	// QRChannelClientOutdated is emitted from GetQRChannel if events.ClientOutdated is received.
	QRChannelClientOutdated = QRChannelItem{Event: "err-client-outdated"}
	// QRChannelScannedWithoutMultidevice is emitted from GetQRChannel if events.QRScannedWithoutMultidevice is received.
	QRChannelScannedWithoutMultidevice = QRChannelItem{Event: "err-scanned-without-multidevice"}
)

type qrChannel struct {
	cli       *Client
	log       waLog.Logger
	ctx       context.Context
	handlerID uint32
	closed    atomic.Bool
	active    atomic.Bool
	outputMu  sync.Mutex
	output    chan<- QRChannelItem
	stopQRs   chan struct{}
	refs      chan [][]byte
	refresh   chan struct{}
	timeout   func(int) time.Duration
}

type qrRefsEvent struct{ refs [][]byte }
type companionRegRefreshEvent struct{}

func (qrc *qrChannel) emit(item QRChannelItem, blocking bool) bool {
	qrc.outputMu.Lock()
	defer qrc.outputMu.Unlock()
	if qrc.closed.Load() {
		return false
	}
	if blocking {
		qrc.output <- item
		return true
	}
	select {
	case qrc.output <- item:
		return true
	default:
		return false
	}
}

func (qrc *qrChannel) finish(item QRChannelItem, disconnect bool) bool {
	qrc.outputMu.Lock()
	if qrc.closed.Swap(true) {
		qrc.outputMu.Unlock()
		return false
	}
	close(qrc.stopQRs)
	qrc.output <- item
	close(qrc.output)
	qrc.outputMu.Unlock()
	go qrc.cli.RemoveEventHandler(qrc.handlerID)
	if disconnect {
		qrc.cli.Disconnect()
	}
	return true
}

func (qrc *qrChannel) emitQRs(refs [][]byte) {
	var currentRef []byte
	for {
		if len(refs) == 0 {
			if qrc.finish(QRChannelTimeout, true) {
				qrc.log.Debugf("Ran out of QR codes, closing channel with status %s and disconnecting client", QRChannelTimeout)
			} else {
				qrc.log.Debugf("Ran out of QR codes, but channel is already closed")
			}
			return
		} else if qrc.closed.Load() {
			qrc.log.Debugf("QR code channel is closed, exiting QR emitter")
			return
		}
		timeout := qrc.timeout(len(refs))
		currentRef, refs = refs[0], refs[1:]
		code := qrc.cli.makeQRData(currentRef, qrc.cli.getQRClientType())
		qrc.log.Debugf("Emitting QR code %s", code)
		qrc.active.Store(true)
		if !qrc.emit(QRChannelItem{Code: code, Timeout: timeout, Event: QRChannelEventCode}, false) {
			qrc.active.Store(false)
			qrc.log.Debugf("Output channel didn't accept code, exiting QR emitter")
			qrc.finish(QRChannelTimeout, true)
			return
		}
		timer := time.NewTimer(timeout)
	waitForRotation:
		for {
			select {
			case <-timer.C:
				break waitForRotation
			case <-qrc.refresh:
				qrc.cli.pairingLock.Lock()
				if qrc.cli.Store.ID != nil || qrc.closed.Load() {
					qrc.cli.pairingLock.Unlock()
					continue
				}
				qrc.cli.Store.AdvSecretKey = random.Bytes(32)
				refreshedCode := qrc.cli.makeQRData(currentRef, qrc.cli.getQRClientType())
				qrc.cli.pairingLock.Unlock()
				if !qrc.emit(QRChannelItem{Code: refreshedCode, Timeout: timeout, Event: QRChannelEventCode}, false) {
					qrc.log.Debugf("Output channel didn't accept refreshed code")
				}
			case <-qrc.stopQRs:
				timer.Stop()
				qrc.log.Debugf("Got signal to stop QR emitter")
				return
			case <-qrc.cli.expectedDisconnect.GetChan():
				timer.Stop()
				qrc.log.Debugf("Client is expected to disconnect, stopping QR emitter")
				return
			case <-qrc.ctx.Done():
				timer.Stop()
				qrc.log.Debugf("Context is done, stopping QR emitter")
				qrc.finish(QRChannelTimeout, true)
				return
			}
		}
	}
}

func qrCodeTimeout(remainingRefs int) time.Duration {
	if remainingRefs == 6 {
		return 60 * time.Second
	}
	return 20 * time.Second
}

func (qrc *qrChannel) handleEvent(rawEvt any) {
	if qrc.closed.Load() {
		qrc.log.Debugf("Dropping event of type %T, channel is closed", rawEvt)
		return
	}
	var outputType QRChannelItem
	switch evt := rawEvt.(type) {
	case *qrRefsEvent:
		qrc.log.Debugf("Received QR refs event, starting to emit codes to channel")
		refs := make([][]byte, len(evt.refs))
		for i := range evt.refs {
			refs[i] = slices.Clone(evt.refs[i])
		}
		select {
		case qrc.refs <- refs:
		default:
		}
		return
	case *companionRegRefreshEvent:
		if !qrc.active.Load() {
			return
		}
		select {
		case qrc.refresh <- struct{}{}:
		default:
		}
		return
	case *events.QRScannedWithoutMultidevice:
		qrc.log.Debugf("QR code scanned without multidevice enabled")
		qrc.emit(QRChannelScannedWithoutMultidevice, true)
		return
	case *events.PairPasskeyRequest:
		qrc.emit(QRChannelItem{
			Event:          QRChannelEventPasskeyRequest,
			PasskeyRequest: evt,
		}, true)
		return
	case *events.PairPasskeyConfirmation:
		if evt.SkipHandoffUX {
			qrc.log.Debugf("Sending automatic passkey confirmation")
			err := qrc.cli.SendPasskeyConfirmation(qrc.ctx)
			if err != nil {
				qrc.emit(QRChannelItem{
					Event: QRChannelEventError,
					Error: fmt.Errorf("failed to send passkey confirmation automatically: %w", err),
				}, true)
			}
		} else {
			qrc.emit(QRChannelItem{
				Event:               QRChannelEventPasskeyResponse,
				PasskeyConfirmation: evt,
			}, true)
		}
		return
	case *events.PairPasskeyError:
		qrc.emit(QRChannelItem{
			Event: QRChannelEventError,
			Error: evt.Error,
		}, true)
		return
	case *events.ClientOutdated:
		outputType = QRChannelClientOutdated
	case *events.PairSuccess:
		outputType = QRChannelSuccess
	case *events.PairError:
		outputType = QRChannelItem{
			Event: QRChannelEventError,
			Error: evt.Error,
		}
	case *events.Disconnected:
		outputType = QRChannelTimeout
	case *events.Connected, *events.ConnectFailure, *events.LoggedOut, *events.TemporaryBan:
		outputType = QRChannelErrUnexpectedEvent
	default:
		return
	}
	if qrc.finish(outputType, false) {
		qrc.log.Debugf("Closing channel with status %+v", outputType)
	} else {
		qrc.log.Debugf("Got status %+v, but channel is already closed", outputType)
	}
}

// GetQRChannel returns a channel that automatically outputs a new QR code when the previous one expires.
//
// This must be called *before* Connect(). It will then listen to all the relevant events from the client.
//
// The last value to be emitted will be a special event like "success", "timeout" or another error code
// depending on the result of the pairing. The channel will be closed immediately after one of those.
func (cli *Client) GetQRChannel(ctx context.Context) (<-chan QRChannelItem, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	} else if cli.IsConnected() {
		return nil, ErrQRAlreadyConnected
	} else if cli.Store.ID != nil {
		return nil, ErrQRStoreContainsID
	}
	ch := make(chan QRChannelItem, 8)
	qrc := qrChannel{
		output:  ch,
		stopQRs: make(chan struct{}),
		cli:     cli,
		log:     cli.Log.Sub("QRChannel"),
		ctx:     ctx,
		refs:    make(chan [][]byte, 1),
		refresh: make(chan struct{}, 16),
		timeout: qrCodeTimeout,
	}
	qrc.handlerID = cli.AddEventHandler(qrc.handleEvent)
	go func() {
		select {
		case refs := <-qrc.refs:
			qrc.emitQRs(refs)
		case <-qrc.stopQRs:
		case <-ctx.Done():
		}
	}()
	return ch, nil
}
