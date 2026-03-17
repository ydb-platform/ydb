//
// CertificateHandlerFactoryMgr.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  CertificateHandlerFactoryMgr
//
// Definition of the CertificateHandlerFactoryMgr class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_CertificateHandlerFactoryMgr_INCLUDED
#define DB_NetSSL_CertificateHandlerFactoryMgr_INCLUDED


#include <map>
#include "DBPoco/Net/CertificateHandlerFactory.h"
#include "DBPoco/Net/NetSSL.h"
#include "DBPoco/SharedPtr.h"


namespace DBPoco
{
namespace Net
{


    class NetSSL_API CertificateHandlerFactoryMgr
    /// A CertificateHandlerFactoryMgr manages all existing CertificateHandlerFactories.
    {
    public:
        typedef std::map<std::string, DBPoco::SharedPtr<CertificateHandlerFactory>> FactoriesMap;

        CertificateHandlerFactoryMgr();
        /// Creates the CertificateHandlerFactoryMgr.

        ~CertificateHandlerFactoryMgr();
        /// Destroys the CertificateHandlerFactoryMgr.

        void setFactory(const std::string & name, CertificateHandlerFactory * pFactory);
        /// Registers the factory. Class takes ownership of the pointer.
        /// If a factory with the same name already exists, an exception is thrown.

        bool hasFactory(const std::string & name) const;
        /// Returns true if for the given name a factory is already registered

        const CertificateHandlerFactory * getFactory(const std::string & name) const;
        /// Returns NULL if for the given name a factory does not exist, otherwise the factory is returned

        void removeFactory(const std::string & name);
        /// Removes the factory from the manager.

    private:
        FactoriesMap _factories;
    };


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_CertificateHandlerFactoryMgr_INCLUDED
