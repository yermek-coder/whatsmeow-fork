package whatsmeow

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func newQRTestClient() *Client {
	device := &store.Device{
		NoiseKey:     keys.NewKeyPair(),
		IdentityKey:  keys.NewKeyPair(),
		AdvSecretKey: []byte("01234567890123456789012345678901"),
	}
	return NewClient(device, waLog.Noop)
}

func qrParts(t *testing.T, code string) []string {
	t.Helper()
	parts := strings.Split(strings.SplitN(code, "#", 2)[1], ",")
	if len(parts) != 5 {
		t.Fatalf("unexpected QR data: %q", code)
	}
	return parts
}

func nextQR(t *testing.T, ch <-chan QRChannelItem) QRChannelItem {
	t.Helper()
	select {
	case item := <-ch:
		return item
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for QR")
		return QRChannelItem{}
	}
}

func TestCompanionRegRefreshChildren(t *testing.T) {
	tests := []struct {
		name    string
		child   string
		refresh bool
	}{
		{name: "companion refresh", child: "companion_reg_refresh", refresh: true},
		{name: "rotate QR", child: "pair-device-rotate-qr", refresh: true},
		{name: "unrelated child", child: "unrelated", refresh: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli := newQRTestClient()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch, err := cli.GetQRChannel(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ref := []byte("current-ref")
			cli.dispatchEvent(&qrRefsEvent{refs: [][]byte{ref, []byte("next-ref")}})
			first := nextQR(t, ch)
			oldSecret := append([]byte(nil), cli.Store.AdvSecretKey...)
			cli.handleCompanionRegRefresh(&waBinary.Node{Content: []waBinary.Node{{Tag: tc.child}}})
			if !tc.refresh {
				select {
				case item := <-ch:
					t.Fatalf("unexpected QR: %+v", item)
				case <-time.After(20 * time.Millisecond):
				}
				if string(cli.Store.AdvSecretKey) != string(oldSecret) {
					t.Fatal("secret changed for invalid child")
				}
				return
			}
			second := nextQR(t, ch)
			if len(cli.Store.AdvSecretKey) != 32 || string(cli.Store.AdvSecretKey) == string(oldSecret) {
				t.Fatal("AdvSecretKey was not replaced with a new 32-byte key")
			}
			firstParts, secondParts := qrParts(t, first.Code), qrParts(t, second.Code)
			if firstParts[0] != secondParts[0] || secondParts[0] != string(ref) {
				t.Fatalf("ref changed across refresh: %q -> %q", firstParts[0], secondParts[0])
			}
			decoded, err := base64.StdEncoding.DecodeString(secondParts[3])
			if err != nil || string(decoded) != string(cli.Store.AdvSecretKey) {
				t.Fatal("refreshed QR does not contain the new AdvSecretKey")
			}
		})
	}
}

func TestCompanionRegRefreshRegisteredAndBeforeFirstQR(t *testing.T) {
	cli := newQRTestClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := cli.GetQRChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldSecret := append([]byte(nil), cli.Store.AdvSecretKey...)
	cli.handleCompanionRegRefresh(&waBinary.Node{Content: []waBinary.Node{{Tag: "companion_reg_refresh"}}})
	cli.dispatchEvent(&qrRefsEvent{refs: [][]byte{[]byte("first"), []byte("second")}})
	first := nextQR(t, ch)
	if qrParts(t, first.Code)[0] != "first" || string(cli.Store.AdvSecretKey) != string(oldSecret) {
		t.Fatal("refresh before first QR was not ignored")
	}

	jid := types.NewJID("123", types.DefaultUserServer)
	cli.Store.ID = &jid
	cli.handleCompanionRegRefresh(&waBinary.Node{Content: []waBinary.Node{{Tag: "pair-device-rotate-qr"}}})
	select {
	case item := <-ch:
		t.Fatalf("registered session received QR: %+v", item)
	case <-time.After(20 * time.Millisecond):
	}
	if string(cli.Store.AdvSecretKey) != string(oldSecret) {
		t.Fatal("registered session secret changed")
	}
}

func TestCompanionRegRefreshDoesNotConsumeRefs(t *testing.T) {
	cli := newQRTestClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan QRChannelItem, 16)
	qrc := &qrChannel{
		cli: cli, log: waLog.Noop, ctx: ctx, output: out, stopQRs: make(chan struct{}),
		refresh: make(chan struct{}, 16), timeout: func(int) time.Duration { return 40 * time.Millisecond },
	}
	go qrc.emitQRs([][]byte{[]byte("first"), []byte("second"), []byte("third")})
	if got := qrParts(t, nextQR(t, out).Code)[0]; got != "first" {
		t.Fatalf("first ref = %q", got)
	}
	qrc.active.Store(true)
	for range 5 {
		qrc.handleEvent(&companionRegRefreshEvent{})
		if got := qrParts(t, nextQR(t, out).Code)[0]; got != "first" {
			t.Fatalf("refresh consumed ref, got %q", got)
		}
	}
	if got := qrParts(t, nextQR(t, out).Code)[0]; got != "second" {
		t.Fatalf("timer rotation ref = %q, want second", got)
	}
	cancel()
}

func TestClosedQRChannelIgnoresRefresh(t *testing.T) {
	cli := newQRTestClient()
	oldSecret := append([]byte(nil), cli.Store.AdvSecretKey...)
	qrc := &qrChannel{
		cli: cli, log: waLog.Noop, output: make(chan QRChannelItem, 1),
		stopQRs: make(chan struct{}), refresh: make(chan struct{}, 1),
	}
	qrc.active.Store(true)
	qrc.closed.Store(true)
	qrc.handleEvent(&companionRegRefreshEvent{})
	if len(qrc.refresh) != 0 || string(cli.Store.AdvSecretKey) != string(oldSecret) {
		t.Fatal("closed QR channel accepted refresh")
	}
}

func TestCompanionRegRefreshNotificationKeepsDeferredAckPath(t *testing.T) {
	cli := newQRTestClient()
	cli.SynchronousAck = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // A canceled context makes the standard ACK path observable without a live socket.
	node := &waBinary.Node{
		Tag: "notification",
		Attrs: waBinary.Attrs{
			"type": "companion_reg_refresh",
			"id":   "notification-id",
			"from": types.ServerJID,
		},
		Content: []waBinary.Node{{Tag: "companion_reg_refresh"}},
	}
	cli.handleNotification(ctx, node)
}
