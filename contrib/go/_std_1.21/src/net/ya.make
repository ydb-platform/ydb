GO_LIBRARY()

NO_COMPILER_WARNINGS()

IF (CGO_ENABLED)
    IF (OS_LINUX)
        CGO_LDFLAGS(-lresolv)
    ENDIF()
ENDIF()

SRCS(
    addrselect.go
    conf.go
    dial.go
    dnsclient.go
    dnsclient_unix.go
    dnsconfig.go
    error_posix.go
    fd_posix.go
    file.go
    hook.go
    hosts.go
    interface.go
    ip.go
    iprawsock.go
    iprawsock_posix.go
    ipsock.go
    ipsock_posix.go
    lookup.go
    mac.go
    net.go
    netcgo_off.go
    netgo_off.go
    nss.go
    parse.go
    pipe.go
    port.go
    rawconn.go
    sock_posix.go
    sockaddr_posix.go
    sockopt_posix.go
    sockoptip_posix.go
    tcpsock.go
    tcpsock_posix.go
    tcpsockopt_posix.go
    udpsock.go
    udpsock_posix.go
    unixsock.go
    unixsock_posix.go
)

IF (OS_LINUX)
    SRCS(
        dnsconfig_unix.go
        error_unix.go
        fd_unix.go
        file_unix.go
        hook_unix.go
        interface_linux.go
        lookup_unix.go
        mptcpsock_linux.go
        port_unix.go
        sendfile_linux.go
        sock_cloexec.go
        sock_linux.go
        sockopt_linux.go
        sockoptip_linux.go
        splice_linux.go
        tcpsockopt_unix.go
        unixsock_readmsg_cmsg_cloexec.go
        writev_unix.go
    )

ENDIF()

IF (OS_LINUX AND CGO_ENABLED)
    SRCS(
        cgo_unix.go
    )

    CGO_SRCS(
        cgo_linux.go
        cgo_resnew.go
        cgo_socknew.go
        cgo_unix_cgo.go
        cgo_unix_cgo_res.go
    )
ELSE()
    IF(OS_LINUX)
        SRCS(
            cgo_stub.go
        )
    ENDIF()
ENDIF()

IF (OS_DARWIN)
    SRCS(
        cgo_darwin.go
        cgo_unix.go
        cgo_unix_syscall.go
        dnsconfig_unix.go
        error_unix.go
        fd_unix.go
        file_unix.go
        hook_unix.go
        interface_bsd.go
        interface_darwin.go
        lookup_unix.go
        mptcpsock_stub.go
        port_unix.go
        sendfile_unix_alt.go
        sock_bsd.go
        sockopt_bsd.go
        sockoptip_bsdvar.go
        splice_stub.go
        sys_cloexec.go
        tcpsockopt_darwin.go
        unixsock_readmsg_cloexec.go
        writev_unix.go
    )

ENDIF()

IF (OS_DARWIN AND CGO_ENABLED)
    CGO_SRCS(cgo_unix_cgo_darwin.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        dnsconfig_windows.go
        error_windows.go
        fd_windows.go
        file_windows.go
        hook_windows.go
        interface_windows.go
        lookup_windows.go
        mptcpsock_stub.go
        sendfile_windows.go
        sock_windows.go
        sockopt_windows.go
        sockoptip_windows.go
        splice_stub.go
        tcpsockopt_windows.go
        unixsock_readmsg_other.go
    )

ENDIF()

END()

RECURSE(
    http
    internal
    mail
    netip
    rpc
    smtp
    textproto
    url
)