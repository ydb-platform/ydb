// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syscall

const (
	SYS_FCNTL         = 72
	SYS_EPOLL_CTL     = 233
	SYS_EPOLL_PWAIT   = 281
	SYS_EPOLL_CREATE1 = 291
	SYS_EPOLL_PWAIT2  = 441

	EPOLLIN       = 0x1
	EPOLLOUT      = 0x4
	EPOLLERR      = 0x8
	EPOLLHUP      = 0x10
	EPOLLRDHUP    = 0x2000
	EPOLLET       = 0x80000000
	EPOLL_CLOEXEC = 0x80000
	EPOLL_CTL_ADD = 0x1
	EPOLL_CTL_DEL = 0x2
	EPOLL_CTL_MOD = 0x3
)

type EpollEvent struct {
	Events uint32
	Data   [8]byte // unaligned uintptr
}
