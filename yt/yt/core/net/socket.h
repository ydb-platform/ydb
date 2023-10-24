#pragma once

#include "address.h"

#include <yt/yt/core/misc/public.h>

#include <util/network/init.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

//! Create NONBLOCKING server socket, set IPV6_ONLY and REUSEADD flags.
SOCKET CreateTcpServerSocket();
SOCKET CreateUnixServerSocket();
SOCKET CreateTcpClientSocket(int family);
SOCKET CreateUnixClientSocket();
SOCKET CreateUdpSocket();

//! Start connect on the socket. Any errors other than EWOULDBLOCK,
//! EAGAIN and EINPROGRESS are thrown as exceptions.
int ConnectSocket(SOCKET clientSocket, const TNetworkAddress& address);

void SetReuseAddrFlag(SOCKET socket);

//! Try binding socket to address. Error is thrown as exception.
void BindSocket(SOCKET serverSocket, const TNetworkAddress& address);

//! Try to accept client on non-blocking socket. Any errors other than
//! EWOULDBLOCK or EAGAIN are thrown as exceptions.
//! Returned socket has CLOEXEC and NONBLOCK flags set.
int AcceptSocket(SOCKET serverSocket, TNetworkAddress* clientAddress);

void ListenSocket(SOCKET serverSocket, int backlog);

void CloseSocket(SOCKET socket);

int GetSocketError(SOCKET socket);

TNetworkAddress GetSocketName(SOCKET socket);
TNetworkAddress GetSocketPeer(SOCKET socket);

bool TrySetSocketNoDelay(SOCKET socket);
bool TrySetSocketKeepAlive(SOCKET socket);
bool TrySetSocketEnableQuickAck(SOCKET socket);
bool TrySetSocketTosLevel(SOCKET socket, int tosLevel);
bool TrySetSocketInputFilter(SOCKET socket, bool drop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
