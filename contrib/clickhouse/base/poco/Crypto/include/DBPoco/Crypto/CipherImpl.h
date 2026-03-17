//
// CipherImpl.h
//
// Library: Crypto
// Package: Cipher
// Module:  CipherImpl
//
// Definition of the CipherImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Crypto_CipherImpl_INCLUDED
#define DB_Crypto_CipherImpl_INCLUDED


#include <openssl/evp.h>
#include "DBPoco/Crypto/Cipher.h"
#include "DBPoco/Crypto/CipherKey.h"
#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Crypto/OpenSSLInitializer.h"


namespace DBPoco
{
namespace Crypto
{


    class CipherImpl : public Cipher
    /// An implementation of the Cipher class for OpenSSL's crypto library.
    {
    public:
        CipherImpl(const CipherKey & key);
        /// Creates a new CipherImpl object for the given CipherKey.

        virtual ~CipherImpl();
        /// Destroys the CipherImpl.

        const std::string & name() const;
        /// Returns the name of the cipher.

        CryptoTransform * createEncryptor();
        /// Creates an encryptor object.

        CryptoTransform * createDecryptor();
        /// Creates a decryptor object.

    private:
        CipherKey _key;
        OpenSSLInitializer _openSSLInitializer;
    };


    //
    // Inlines
    //
    inline const std::string & CipherImpl::name() const
    {
        return _key.name();
    }


}
} // namespace DBPoco::Crypto


#endif // DB_Crypto_CipherImpl_INCLUDED
