//
// ServerSocketImpl.h
//
// Library: Net
// Package: Sockets
// Module:  ServerSocketImpl
//
// Definition of the ServerSocketImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_ServerSocketImpl_INCLUDED
#define CHDB_Net_ServerSocketImpl_INCLUDED


#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/Net/SocketImpl.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API ServerSocketImpl : public SocketImpl
    /// This class implements a TCP server socket.
    {
    public:
        ServerSocketImpl();
        /// Creates the ServerSocketImpl.

    protected:
        virtual ~ServerSocketImpl();
        /// Destroys the ServerSocketImpl.
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_ServerSocketImpl_INCLUDED
