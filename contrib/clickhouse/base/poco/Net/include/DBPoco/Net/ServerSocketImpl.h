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


#ifndef DB_Net_ServerSocketImpl_INCLUDED
#define DB_Net_ServerSocketImpl_INCLUDED


#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketImpl.h"


namespace DBPoco
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
} // namespace DBPoco::Net


#endif // DB_Net_ServerSocketImpl_INCLUDED
