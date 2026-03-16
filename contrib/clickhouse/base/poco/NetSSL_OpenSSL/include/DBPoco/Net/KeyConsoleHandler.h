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


#ifndef DB_NetSSL_KeyConsoleHandler_INCLUDED
#define DB_NetSSL_KeyConsoleHandler_INCLUDED


#include "DBPoco/Net/NetSSL.h"
#include "DBPoco/Net/PrivateKeyPassphraseHandler.h"


namespace DBPoco
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
} // namespace DBPoco::Net


#endif // DB_NetSSL_KeyConsoleHandler_INCLUDED
