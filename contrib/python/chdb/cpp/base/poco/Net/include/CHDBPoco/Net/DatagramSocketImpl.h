//
// DatagramSocketImpl.h
//
// Library: Net
// Package: Sockets
// Module:  DatagramSocketImpl
//
// Definition of the DatagramSocketImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_DatagramSocketImpl_INCLUDED
#define CHDB_Net_DatagramSocketImpl_INCLUDED


#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketImpl.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API DatagramSocketImpl : public SocketImpl
    /// This class implements an UDP socket.
    {
    public:
        DatagramSocketImpl();
        /// Creates an unconnected, unbound datagram socket.

        explicit DatagramSocketImpl(SocketAddress::Family family);
        /// Creates an unconnected datagram socket.
        ///
        /// The socket will be created for the
        /// given address family.

        DatagramSocketImpl(CHDB_poco_socket_t sockfd);
        /// Creates a StreamSocketImpl using the given native socket.

    protected:
        void init(int af);

        ~DatagramSocketImpl();
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_DatagramSocketImpl_INCLUDED
