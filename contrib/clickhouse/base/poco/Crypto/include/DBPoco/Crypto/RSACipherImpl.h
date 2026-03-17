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


#ifndef DB_Crypto_RSACipherImpl_INCLUDED
#define DB_Crypto_RSACipherImpl_INCLUDED


#include <openssl/evp.h>
#include "DBPoco/Crypto/Cipher.h"
#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Crypto/OpenSSLInitializer.h"
#include "DBPoco/Crypto/RSAKey.h"


namespace DBPoco
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
} // namespace DBPoco::Crypto


#endif // DB_Crypto_RSACipherImpl_INCLUDED
