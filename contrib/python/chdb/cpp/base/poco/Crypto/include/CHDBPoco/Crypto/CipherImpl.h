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


#ifndef CHDB_Crypto_CipherImpl_INCLUDED
#define CHDB_Crypto_CipherImpl_INCLUDED


#include <openssl/evp.h>
#include "CHDBPoco/Crypto/Cipher.h"
#include "CHDBPoco/Crypto/CipherKey.h"
#include "CHDBPoco/Crypto/Crypto.h"
#include "CHDBPoco/Crypto/OpenSSLInitializer.h"


namespace CHDBPoco
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
} // namespace CHDBPoco::Crypto


#endif // CHDB_Crypto_CipherImpl_INCLUDED
