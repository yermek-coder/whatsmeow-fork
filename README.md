# whatsmeow
[![Go Reference](https://pkg.go.dev/badge/go.mau.fi/whatsmeow.svg)](https://pkg.go.dev/go.mau.fi/whatsmeow)

whatsmeow is a Go library for the WhatsApp web multidevice API.

## Why this fork exists

This fork was created on August 18, 2026 to fix companion registration QR
refresh handling (commit [`b1107e4`](https://github.com/yermek-coder/whatsmeow-fork/commit/b1107e40b3d1def45810abb6418eda930a983850)).
It keeps the upstream module path, `go.mau.fi/whatsmeow`.

**Status as of November 8, 2026:** keep the fork for applications that depend on
its `GetQRChannel` refresh behavior. It has been merged with upstream `main` at
[`79dc2a8`](https://github.com/tulir/whatsmeow/commit/79dc2a8).
Upstream added companion refresh support in
[`9ec8f76`](https://github.com/tulir/whatsmeow/commit/9ec8f76), but the implementations
still differ in these ways:

* **Keep the original QR expiry window.** The fork refreshes the current QR
  using the same server reference without restarting its timer or consuming
  another reference. Upstream requeues the current code and starts another
  timeout, so repeated refreshes can extend that reference's lifetime.
* **Limit when the ADV secret changes.** Refresh requests are handled only by
  an active QR channel and ignored before the first QR, after channel closure,
  or after the device is registered. Secret rotation shares a lock with pairing
  to avoid changing the secret while pairing validates the device identity.
  Upstream rotates the secret directly from the notification handler.
* **Rebuild QR data from server references.** Each QR is generated with the
  current secret instead of replacing secret text inside prebuilt QR strings.
  The public `RotateADVSecret` event is still emitted on refresh.
* **Serialize channel output and closure.** The fork coordinates QR emission
  and terminal events to prevent concurrent sends and channel closure, and
  starts one emitter per QR channel.

The fork includes regression tests in `qrchan_test.go` for both refresh child
tags (`companion_reg_refresh` and `pair-device-rotate-qr`), secret/QR consistency,
reference preservation, ignored refreshes, and the standard notification ACK
path. These are local tests; they do not establish live WhatsApp compatibility.

The fork can be retired once upstream provides equivalent behavior and the
refresh tests pass against it, followed by a live pairing check in the consuming
application. Applications that do not need these safeguards may use upstream
directly; the remaining fork changes are confined to QR pairing and its tests.

## Discussion
Matrix room: [#whatsmeow:maunium.net](https://matrix.to/#/#whatsmeow:maunium.net)

For questions about the WhatsApp protocol (like how to send a specific type of
message), you can also use the [WhatsApp protocol Q&A] section on GitHub
discussions.

[WhatsApp protocol Q&A]: https://github.com/tulir/whatsmeow/discussions/categories/whatsapp-protocol-q-a

## Usage
The [godoc](https://pkg.go.dev/go.mau.fi/whatsmeow) includes docs for all methods and event types.
There's also a [simple example](https://pkg.go.dev/go.mau.fi/whatsmeow#example-package) at the top.

## Features
Most core features are already present:

* Sending messages to private chats and groups (both text and media)
* Receiving all messages
* Managing groups and receiving group change events
* Joining via invite messages, using and creating invite links
* Sending and receiving typing notifications
* Sending and receiving delivery and read receipts
* Reading and writing app state (contact list, chat pin/mute status, etc)
* Sending and handling retry receipts if message decryption fails
* Sending status messages (experimental, may not work for large contact lists)

Things that are not yet implemented:

* Sending broadcast list messages (this is not supported on WhatsApp web either)
* Calls
