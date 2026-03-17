//
// KeyPairImpl.h
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPairImpl
//
// Definition of the KeyPairImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Crypto_KeyPairImplImpl_INCLUDED
#define DB_Crypto_KeyPairImplImpl_INCLUDED


#include <string>
#include <vector>
#include "DBPoco/AutoPtr.h"
#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Crypto/OpenSSLInitializer.h"
#include "DBPoco/RefCountedObject.h"


namespace DBPoco
{
namespace Crypto
{


    class KeyPairImpl : public DBPoco::RefCountedObject
    /// Class KeyPairImpl
    {
    public:
        enum Type
        {
            KT_RSA_IMPL = 0,
            KT_EC_IMPL
        };

        typedef DBPoco::AutoPtr<KeyPairImpl> Ptr;
        typedef std::vector<unsigned char> ByteVec;

        KeyPairImpl(const std::string & name, Type type);
        /// Create KeyPairImpl with specified type and name.

        virtual ~KeyPairImpl();
        /// Destroys the KeyPairImpl.

        virtual int size() const = 0;
        /// Returns the key size.

        const std::string & name() const;
        /// Returns key pair name

        Type type() const;
        /// Returns key pair type

    private:
        KeyPairImpl();

        std::string _name;
        Type _type;
        OpenSSLInitializer _openSSLInitializer;
    };


    //
    // inlines
    //


    inline const std::string & KeyPairImpl::name() const
    {
        return _name;
    }


    inline KeyPairImpl::Type KeyPairImpl::type() const
    {
        return _type;
    }


}
} // namespace DBPoco::Crypto


#endif // DB_Crypto_KeyPairImplImpl_INCLUDED
