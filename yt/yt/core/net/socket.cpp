#include "socket.h"
#include "address.h"

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/system/env.h>
#include <util/system/shellcommand.h>

#ifdef _unix_
    #include <netinet/ip.h>
    #include <netinet/tcp.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

#ifdef _win_
    #include <util/network/socket.h>

    #include <winsock2.h>
#endif

#ifdef _linux_
    #include <linux/filter.h>
#endif

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

void SetReuseAddrFlag(SOCKET socket)
{
    int flag = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (const char*) &flag, sizeof(flag)) != 0) {
        auto lastError = LastSystemError();
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to configure socket address reuse")
            << TError::FromSystem(lastError);
    }
}

void SetReusePortFlag(SOCKET socket)
{
#ifdef SO_REUSEPORT
    int flag = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_REUSEPORT, (const char*) &flag, sizeof(flag)) != 0) {
        auto lastError = LastSystemError();
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to configure socket port reuse")
            << TError::FromSystem(lastError);
    }
#endif
}

SOCKET CreateTcpServerSocket()
{
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET serverSocket = socket(AF_INET6, type, IPPROTO_TCP);
    if (serverSocket == INVALID_SOCKET) {
        auto lastError = LastSystemError();
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create a server socket")
            << TError::FromSystem(lastError);
    }

#ifdef _win_
    SetNonBlock(serverSocket);
#endif

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(serverSocket, F_GETFL);
        int result = fcntl(serverSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(serverSocket, F_GETFD);
        int result = fcntl(serverSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#endif

    {
        int flag = 0;
        if (setsockopt(serverSocket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &flag, sizeof(flag)) != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to configure IPv6 protocol")
                << TError::FromSystem(lastError);
        }
    }

    try {
        SetReuseAddrFlag(serverSocket);
        // SO_REUSEPORT is necessary for Darwin build.
        // More details here: https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ
#ifdef _darwin_
        SetReusePortFlag(serverSocket);
#endif
    } catch (...) {
        SafeClose(serverSocket, false);
        throw;
    }

    return serverSocket;
}

SOCKET CreateUnixServerSocket()
{
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET serverSocket = socket(AF_UNIX, type, 0);
    if (serverSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create a local server socket")
            << TError::FromSystem();
    }

#ifdef _win_
    SetNonBlock(serverSocket);
#endif

    return serverSocket;
}

SOCKET CreateTcpClientSocket(int family)
{
    YT_VERIFY(family == AF_INET6 || family == AF_INET);

    int protocol = IPPROTO_TCP;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET clientSocket = socket(family, type, protocol);
    if (clientSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create client socket")
            << TError::FromSystem();
    }

    if (family == AF_INET6) {
        int value = 0;
        if (setsockopt(clientSocket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &value, sizeof(value)) != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to configure IPv6 protocol")
                << TError::FromSystem(lastError);
        }
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#elif defined _win_
    SetNonBlock(clientSocket);
#endif

    return clientSocket;
}

SOCKET CreateUnixClientSocket()
{
    int family = AF_UNIX;
    int protocol = 0;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET clientSocket = socket(family, type, protocol);
    if (clientSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create client socket")
            << TError::FromSystem();
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#elif defined _win_
    SetNonBlock(clientSocket);
#endif

    return clientSocket;
}

SOCKET CreateUdpSocket()
{
    int type = SOCK_DGRAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET udpSocket = socket(AF_INET6, type, 0);
    if (udpSocket == INVALID_SOCKET) {
        auto lastError = LastSystemError();
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create a server socket")
            << TError::FromSystem(lastError);
    }

#ifdef _win_
    SetNonBlock(udpSocket);
#endif

    return udpSocket;
}

int ConnectSocket(SOCKET clientSocket, const TNetworkAddress& address)
{
    int result = HandleEintr(connect, clientSocket, address.GetSockAddr(), address.GetLength());
    if (result != 0) {
        int error = LastSystemError();
        if (error != EAGAIN && error != EWOULDBLOCK && error != EINPROGRESS) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Error connecting to %v",
                address)
                << TError::FromSystem(error);
        }
    }

    return result;
}

void BindSocket(SOCKET serverSocket, const TNetworkAddress& address)
{
    if (bind(serverSocket, address.GetSockAddr(), address.GetLength()) != 0) {
        if (GetEnv("YT_DEBUG_TAKEN_PORT")) {
            try {
                TShellCommand cmd("ss -tlpn");
                cmd.Run();
                Cerr << cmd.GetOutput() << Endl;

                TShellCommand cmd2("ss -tpn");
                cmd2.Run();
                Cerr << cmd2.GetOutput() << Endl;
            } catch (const std::exception& ex) {
                Cerr << ex.what() << Endl;
            }
        }

        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to bind a server socket to %v",
            address)
            << TError::FromSystem();
    }
}

int AcceptSocket(SOCKET serverSocket, TNetworkAddress* clientAddress)
{
    SOCKET clientSocket;

#ifdef _linux_
    clientSocket = accept4(
        serverSocket,
        clientAddress->GetSockAddr(),
        clientAddress->GetLengthPtr(),
        SOCK_CLOEXEC | SOCK_NONBLOCK);
#else
    clientSocket = accept(
        serverSocket,
        clientAddress->GetSockAddr(),
        clientAddress->GetLengthPtr());
#endif

    if (clientSocket == INVALID_SOCKET) {
        auto error = LastSystemError();
        if (error != EAGAIN && error != EWOULDBLOCK && error != ECONNABORTED && error != EMFILE) {
            // ECONNABORTED means, that a socket on the listen
            // queue was closed before we Accept()ed it; ignore it.
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Error accepting connection")
                << TError::FromSystem();
        }

        return clientSocket;
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#elif defined _win_
    SetNonBlock(clientSocket);
#endif

    return clientSocket;
}

void ListenSocket(SOCKET serverSocket, int backlog)
{
    if (listen(serverSocket, backlog) == -1) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to listen to server socket")
            << TError::FromSystem();
    }
}

void CloseSocket(SOCKET socket)
{
    YT_VERIFY(close(socket) == 0);
}

int GetSocketError(SOCKET socket)
{
    int error;
    socklen_t errorLen = sizeof (error);
    getsockopt(socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &errorLen);
    return error;
}

TNetworkAddress GetSocketName(SOCKET socket)
{
    TNetworkAddress address;
    auto lengthPtr = address.GetLengthPtr();
    int result = getsockname(socket, address.GetSockAddr(), lengthPtr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to get socket name")
            << TError::FromSystem();
    }

    return address;
}

TNetworkAddress GetSocketPeerName(SOCKET socket)
{
    TNetworkAddress address;
    auto lengthPtr = address.GetLengthPtr();
    int result = getpeername(socket, address.GetSockAddr(), lengthPtr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to get socket peer name")
            << TError::FromSystem();
    }

    return address;
}

bool TrySetSocketNoDelay(SOCKET socket)
{
    int value = 1;
    if (setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
    return true;
}

bool TrySetSocketKeepAlive(SOCKET socket)
{
#ifdef _linux_
    int value = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
#else
    Y_UNUSED(socket);
#endif
    return true;
}

bool TrySetSocketEnableQuickAck(SOCKET socket)
{
#ifdef _linux_
    int value = 1;
    if (setsockopt(socket, IPPROTO_TCP, TCP_QUICKACK, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
#else
    Y_UNUSED(socket);
#endif
    return true;
}

bool TrySetSocketTosLevel(SOCKET socket, int tosLevel)
{
#ifdef _unix_
    if (setsockopt(socket, IPPROTO_IP, IP_TOS, &tosLevel, sizeof(tosLevel)) != 0) {
        return false;
    }
    if (setsockopt(socket, IPPROTO_IPV6, IPV6_TCLASS, &tosLevel, sizeof(tosLevel)) != 0) {
        return false;
    }
    return true;
#else
    return false;
#endif
}

bool TrySetSocketInputFilter(SOCKET socket, bool drop)
{
#ifdef _linux_
    static struct sock_filter filter_code[] = {
        BPF_STMT(BPF_RET, 0),
    };
    static const struct sock_fprog filter = {
        .len = Y_ARRAY_SIZE(filter_code),
        .filter = filter_code,
    };

    return setsockopt(socket, SOL_SOCKET, drop ? SO_ATTACH_FILTER : SO_DETACH_FILTER, &filter, sizeof(filter)) == 0;
#else
    Y_UNUSED(socket);
    Y_UNUSED(drop);
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
