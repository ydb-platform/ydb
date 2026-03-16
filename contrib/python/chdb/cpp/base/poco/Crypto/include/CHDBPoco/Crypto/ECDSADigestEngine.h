//
// ECDSADigestEngine.h
//
//
// Library: Crypto
// Package: ECDSA
// Module:  ECDSADigestEngine
//
// Definition of the ECDSADigestEngine class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Crypto_ECDSADigestEngine_INCLUDED
#define CHDB_Crypto_ECDSADigestEngine_INCLUDED


#include <istream>
#include <ostream>
#include "CHDBPoco/Crypto/Crypto.h"
#include "CHDBPoco/Crypto/DigestEngine.h"
#include "CHDBPoco/Crypto/ECKey.h"
#include "CHDBPoco/DigestEngine.h"


namespace CHDBPoco
{
namespace Crypto
{


    class Crypto_API ECDSADigestEngine : public CHDBPoco::DigestEngine
    /// This class implements a CHDBPoco::DigestEngine that can be
    /// used to compute a secure digital signature.
    ///
    /// First another CHDBPoco::Crypto::DigestEngine is created and
    /// used to compute a cryptographic hash of the data to be
    /// signed. Then, the hash value is encrypted, using
    /// the ECDSA private key.
    ///
    /// To verify a signature, pass it to the verify()
    /// member function. It will decrypt the signature
    /// using the ECDSA public key and compare the resulting
    /// hash with the actual hash of the data.
    {
    public:
        ECDSADigestEngine(const ECKey & key, const std::string & name);
        /// Creates the ECDSADigestEngine with the given ECDSA key,
        /// using the hash algorithm with the given name
        /// (e.g., "SHA1", "SHA256", "SHA512", etc.).
        /// See the OpenSSL documentation for a list of supported digest algorithms.
        ///
        /// Throws a CHDBPoco::NotFoundException if no algorithm with the given name exists.

        ~ECDSADigestEngine();
        /// Destroys the ECDSADigestEngine.

        std::size_t digestLength() const;
        /// Returns the length of the digest in bytes.

        void reset();
        /// Resets the engine so that a new
        /// digest can be computed.

        const DigestEngine::Digest & digest();
        /// Finishes the computation of the digest
        /// (the first time it's called) and
        /// returns the message digest.
        ///
        /// Can be called multiple times.

        const DigestEngine::Digest & signature();
        /// Signs the digest using the ECDSADSA algorithm
        /// and the private key (the first time it's
        /// called) and returns the result.
        ///
        /// Can be called multiple times.

        bool verify(const DigestEngine::Digest & signature);
        /// Verifies the data against the signature.
        ///
        /// Returns true if the signature can be verified, false otherwise.

    protected:
        void updateImpl(const void * data, std::size_t length);

    private:
        ECKey _key;
        CHDBPoco::Crypto::DigestEngine _engine;
        CHDBPoco::DigestEngine::Digest _digest;
        CHDBPoco::DigestEngine::Digest _signature;
    };


}
} // namespace CHDBPoco::Crypto


#endif // CHDB_Crypto_ECDSADigestEngine_INCLUDED
