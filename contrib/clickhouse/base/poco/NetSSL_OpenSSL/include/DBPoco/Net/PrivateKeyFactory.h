//
// PrivateKeyFactory.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  PrivateKeyFactory
//
// Definition of the PrivateKeyFactory class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_PrivateKeyFactory_INCLUDED
#define DB_NetSSL_PrivateKeyFactory_INCLUDED


#include "DBPoco/Net/NetSSL.h"


namespace DBPoco
{
namespace Net
{


    class PrivateKeyPassphraseHandler;


    class NetSSL_API PrivateKeyFactory
    /// A PrivateKeyFactory is responsible for creating PrivateKeyPassphraseHandlers.
    ///
    /// You don't need to access this class directly. Use the macro
    ///     DB_POCO_REGISTER_KEYFACTORY(namespace, PrivateKeyPassphraseHandlerName)
    /// instead (see the documentation of PrivateKeyPassphraseHandler for an example).
    {
    public:
        PrivateKeyFactory();
        /// Creates the PrivateKeyFactory.

        virtual ~PrivateKeyFactory();
        /// Destroys the PrivateKeyFactory.

        virtual PrivateKeyPassphraseHandler * create(bool onServer) const = 0;
        /// Creates a new PrivateKeyPassphraseHandler
    };


    class NetSSL_API PrivateKeyFactoryRegistrar
    /// Registrar class which automatically registers PrivateKeyFactories at the PrivateKeyFactoryMgr.
    ///
    /// You don't need to access this class directly. Use the macro
    ///     DB_POCO_REGISTER_KEYFACTORY(namespace, PrivateKeyPassphraseHandlerName)
    /// instead (see the documentation of PrivateKeyPassphraseHandler for an example).

    {
    public:
        PrivateKeyFactoryRegistrar(const std::string & name, PrivateKeyFactory * pFactory);
        /// Registers the PrivateKeyFactory with the given name at the factory manager.

        virtual ~PrivateKeyFactoryRegistrar();
        /// Destroys the PrivateKeyFactoryRegistrar.
    };


    template <typename T>
    class PrivateKeyFactoryImpl : public DBPoco::Net::PrivateKeyFactory
    {
    public:
        PrivateKeyFactoryImpl() { }

        ~PrivateKeyFactoryImpl() { }

        PrivateKeyPassphraseHandler * create(bool server) const { return new T(server); }
    };


}
} // namespace DBPoco::Net


// DEPRECATED: register the factory directly at the FactoryMgr:
// DBPoco::Net::SSLManager::instance().privateKeyFactoryMgr().setFactory(name, new DBPoco::Net::PrivateKeyFactoryImpl<MyKeyHandler>());
#define DB_POCO_REGISTER_KEYFACTORY(API, PKCLS) \
    static DBPoco::Net::PrivateKeyFactoryRegistrar aRegistrar(std::string(#PKCLS), new DBPoco::Net::PrivateKeyFactoryImpl<PKCLS>());


#endif // DB_NetSSL_PrivateKeyFactory_INCLUDED
