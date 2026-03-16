//
// CertificateHandlerFactory.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  CertificateHandlerFactory
//
// Definition of the CertificateHandlerFactory class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_NetSSL_CertificateHandlerFactory_INCLUDED
#define CHDB_NetSSL_CertificateHandlerFactory_INCLUDED


#include "CHDBPoco/Net/NetSSL.h"


namespace CHDBPoco
{
namespace Net
{


    class InvalidCertificateHandler;


    class NetSSL_API CertificateHandlerFactory
    /// A CertificateHandlerFactory is responsible for creating InvalidCertificateHandlers.
    ///
    /// You don't need to access this class directly. Use the macro
    ///     CHDB_POCO_REGISTER_CHFACTORY(namespace, InvalidCertificateHandlerName)
    /// instead (see the documentation of InvalidCertificateHandler for an example).
    {
    public:
        CertificateHandlerFactory();
        /// Creates the CertificateHandlerFactory.

        virtual ~CertificateHandlerFactory();
        /// Destroys the CertificateHandlerFactory.

        virtual InvalidCertificateHandler * create(bool server) const = 0;
        /// Creates a new InvalidCertificateHandler. Set server to true if the certificate handler is used on the server side.
    };


    class NetSSL_API CertificateHandlerFactoryRegistrar
    /// Registrar class which automatically registers CertificateHandlerFactory at the CertificateHandlerFactoryMgr.
    /// You don't need to access this class directly. Use the macro
    ///     CHDB_POCO_REGISTER_CHFACTORY(namespace, InvalidCertificateHandlerName)
    /// instead (see the documentation of InvalidCertificateHandler for an example).
    {
    public:
        CertificateHandlerFactoryRegistrar(const std::string & name, CertificateHandlerFactory * pFactory);
        /// Registers the CertificateHandlerFactory with the given name at the factory manager.

        virtual ~CertificateHandlerFactoryRegistrar();
        /// Destroys the CertificateHandlerFactoryRegistrar.
    };


    template <typename T>
    class CertificateHandlerFactoryImpl : public CHDBPoco::Net::CertificateHandlerFactory
    {
    public:
        CertificateHandlerFactoryImpl() { }

        ~CertificateHandlerFactoryImpl() { }

        InvalidCertificateHandler * create(bool server) const { return new T(server); }
    };


}
} // namespace CHDBPoco::Net


// DEPRECATED: register the factory directly at the FactoryMgr:
// CHDBPoco::Net::SSLManager::instance().certificateHandlerFactoryMgr().setFactory(name, new CHDBPoco::Net::CertificateHandlerFactoryImpl<MyConsoleHandler>());
#define CHDB_POCO_REGISTER_CHFACTORY(API, PKCLS) \
    static CHDBPoco::Net::CertificateHandlerFactoryRegistrar aRegistrar( \
        std::string(#PKCLS), new CHDBPoco::Net::CertificateHandlerFactoryImpl<PKCLS>());


#endif // CHDB_NetSSL_CertificateHandlerFactory_INCLUDED
