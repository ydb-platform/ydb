//
// RSACipherImpl.h
//
// Library: Crypto
// Package: RSA
// Module:  RSACipherImpl
//
// Definition of the RSACipherImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Crypto_RSACipherImpl_INCLUDED
#define CHDB_Crypto_RSACipherImpl_INCLUDED


#include <openssl/evp.h>
#include "CHDBPoco/Crypto/Cipher.h"
#include "CHDBPoco/Crypto/Crypto.h"
#include "CHDBPoco/Crypto/OpenSSLInitializer.h"
#include "CHDBPoco/Crypto/RSAKey.h"


namespace CHDBPoco
{
namespace Crypto
{


    class RSACipherImpl : public Cipher
    /// An implementation of the Cipher class for
    /// asymmetric (public-private key) encryption
    /// based on the the RSA algorithm in OpenSSL's
    /// crypto library.
    ///
    /// Encryption is using the public key, decryption
    /// requires the private key.
    {
    public:
        RSACipherImpl(const RSAKey & key, RSAPaddingMode paddingMode);
        /// Creates a new RSACipherImpl object for the given RSAKey
        /// and using the given padding mode.

        virtual ~RSACipherImpl();
        /// Destroys the RSACipherImpl.

        const std::string & name() const;
        /// Returns the name of the Cipher.

        CryptoTransform * createEncryptor();
        /// Creates an encryptor object.

        CryptoTransform * createDecryptor();
        /// Creates a decryptor object.

    private:
        RSAKey _key;
        RSAPaddingMode _paddingMode;
        OpenSSLInitializer _openSSLInitializer;
    };


    //
    // Inlines
    //
    inline const std::string & RSACipherImpl::name() const
    {
        return _key.name();
    }


}
} // namespace CHDBPoco::Crypto


#endif // CHDB_Crypto_RSACipherImpl_INCLUDED
