//
// KeyConsoleHandler.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  KeyConsoleHandler
//
// Definition of the KeyConsoleHandler class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_NetSSL_KeyConsoleHandler_INCLUDED
#define CHDB_NetSSL_KeyConsoleHandler_INCLUDED


#include "CHDBPoco/Net/NetSSL.h"
#include "CHDBPoco/Net/PrivateKeyPassphraseHandler.h"


namespace CHDBPoco
{
namespace Net
{


    class NetSSL_API KeyConsoleHandler : public PrivateKeyPassphraseHandler
    /// An implementation of PrivateKeyPassphraseHandler that
    /// reads the key for a certificate from the console.
    {
    public:
        KeyConsoleHandler(bool server);
        /// Creates the KeyConsoleHandler.

        ~KeyConsoleHandler();
        /// Destroys the KeyConsoleHandler.

        void onPrivateKeyRequested(const void * pSender, std::string & privateKey);
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_NetSSL_KeyConsoleHandler_INCLUDED
