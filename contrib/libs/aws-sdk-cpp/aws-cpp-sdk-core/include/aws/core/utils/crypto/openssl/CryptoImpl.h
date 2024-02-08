/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#pragma once

#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/crypto/HMAC.h>
#include <aws/core/utils/crypto/Cipher.h>
#include <aws/core/utils/crypto/SecureRandom.h>
#include <aws/core/utils/GetTheLights.h>
#include <openssl/ossl_typ.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <atomic>
#include <mutex>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            namespace OpenSSL
            {
                extern GetTheLights getTheLights;

                void init_static_state();

                void cleanup_static_state();

                void locking_fn(int mode, int n, const char* file, int line);

                unsigned long id_fn();
            }

            /**
             * OpenSSL implementation for SecureRandomBytes.
             * Incidentally, this implementation is thread safe, though it is not
             * on other platforms. You should treat an instance of SecureRandomBytes
             * as needed to be memory fenced if you will be using across multiple threads
             */
            class SecureRandomBytes_OpenSSLImpl : public SecureRandomBytes
            {
            public:
                SecureRandomBytes_OpenSSLImpl()
                { }

                ~SecureRandomBytes_OpenSSLImpl() = default;

                /**
                 * Reads bufferSize bytes from RAND_bytes into buffer.
                 */
                void GetBytes(unsigned char* buffer, size_t bufferSize) override;
            };

            class MD5OpenSSLImpl : public Hash
            {
            public:

                MD5OpenSSLImpl();

                virtual ~MD5OpenSSLImpl();

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                virtual HashResult GetHash() override;

            private:
                EVP_MD_CTX *m_ctx;
            };

            class Sha1OpenSSLImpl : public Hash
            {
            public:

                Sha1OpenSSLImpl();

                virtual ~Sha1OpenSSLImpl();

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                virtual HashResult GetHash() override;

            private:
                EVP_MD_CTX *m_ctx;
            };

            class Sha256OpenSSLImpl : public Hash
            {
            public:
                Sha256OpenSSLImpl();

                virtual ~Sha256OpenSSLImpl();

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                virtual HashResult GetHash() override;

            private:
                EVP_MD_CTX *m_ctx;
            };

            class Sha256HMACOpenSSLImpl : public HMAC
            {
            public:
                virtual ~Sha256HMACOpenSSLImpl() = default;

                virtual HashResult Calculate(const ByteBuffer& toSign, const ByteBuffer& secret) override;
            };

            /**
             * OpenSSL implementation for SymmetricCipher
             */
            class OpenSSLCipher : public SymmetricCipher
            {
            public:
                /**
                 * Creates new OpenSSL based cipher for key, and autogenerates a secure IV of size ivSize
                 */
                OpenSSLCipher(const CryptoBuffer& key, size_t ivSize, bool ctrMode = false);

                /**
                 * Creates new OpenSSL based cipher for key, initializationVector, and optional tag. If this is an authenticated
                 * cipher being used for decryption.
                 */
                OpenSSLCipher(CryptoBuffer&& key, CryptoBuffer&& initializationVector,
                              CryptoBuffer&& tag = CryptoBuffer(0));

                /**
                 * Creates new OpenSSL based cipher for key, initializationVector, and optional tag. If this is an authenticated
                 * cipher being used for decryption.
                 */
                OpenSSLCipher(const CryptoBuffer& key, const CryptoBuffer& initializationVector,
                              const CryptoBuffer& tag = CryptoBuffer(0));

                OpenSSLCipher(const OpenSSLCipher& other) = delete;

                OpenSSLCipher& operator=(const OpenSSLCipher& other) = delete;

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                OpenSSLCipher(OpenSSLCipher&& toMove);

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                OpenSSLCipher& operator=(OpenSSLCipher&& toMove) = default;


                virtual ~OpenSSLCipher();

                /**
                 * Encrypt a buffer of data. Part of the contract for this interface is that intention that
                 * a user call this function multiple times for a large stream. As such, multiple calls to this function
                 * on the same instance should produce valid sequential output for an encrypted stream.
                 */
                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;

                /**
                 * Finalize Encryption, returns anything remaining in the last block
                 */
                CryptoBuffer FinalizeEncryption() override;

                /**
                * Decrypt a buffer of data. Part of the contract for this interface is that intention that
                * a user call this function multiple times for a large stream. As such, multiple calls to this function
                * on the same instance should produce valid sequential output from an encrypted stream.
                */
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;

                /**
                 * Finalize Decryption, returns anything remaining in the last block
                 */
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                virtual size_t GetBlockSizeBytes() const = 0;
                virtual size_t GetKeyLengthBits() const = 0;
                bool CheckKeyAndIVLength(size_t expectedKeyLength, size_t expectedIVLength);

                EVP_CIPHER_CTX* m_encryptor_ctx;
                EVP_CIPHER_CTX* m_decryptor_ctx;
            private:
                void Init();
                void Cleanup();

                /* openssl has bug finalize decryption of an empty string */
                bool m_emptyPlaintext = false;
            };

            /**
             * OpenSSL implementation for AES in CBC mode
             */
            class AES_CBC_Cipher_OpenSSL : public OpenSSLCipher
            {
            public:
                /**
                 * Create AES in CBC mode off of a 256 bit key. Auto Generates a 16 byte secure random IV
                 */
                AES_CBC_Cipher_OpenSSL(const CryptoBuffer& key);

                /**
                 * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                 */
                AES_CBC_Cipher_OpenSSL(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                 * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                 */
                AES_CBC_Cipher_OpenSSL(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CBC_Cipher_OpenSSL(const AES_CBC_Cipher_OpenSSL& other) = delete;

                AES_CBC_Cipher_OpenSSL& operator=(const AES_CBC_Cipher_OpenSSL& other) = delete;

                AES_CBC_Cipher_OpenSSL(AES_CBC_Cipher_OpenSSL&& toMove) = default;

                void Reset() override;

            protected:
                size_t GetBlockSizeBytes() const override;

                size_t GetKeyLengthBits() const override;

            private:
                void InitCipher();

                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;
            };

            /**
             * OpenSSL implementation for AES in CTR mode
             */
            class AES_CTR_Cipher_OpenSSL : public OpenSSLCipher
            {
            public:
                /**
                 * Create AES in CTR mode off of a 256 bit key. Auto Generates a 16 byte IV in the format
                 * [nonce 4bytes ] [securely random iv 8 bytes] [ CTR init 4bytes ]
                 */
                AES_CTR_Cipher_OpenSSL(const CryptoBuffer& key);

                /**
                 * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                 */
                AES_CTR_Cipher_OpenSSL(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                 * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                 */
                AES_CTR_Cipher_OpenSSL(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CTR_Cipher_OpenSSL(const AES_CTR_Cipher_OpenSSL& other) = delete;

                AES_CTR_Cipher_OpenSSL& operator=(const AES_CTR_Cipher_OpenSSL& other) = delete;

                AES_CTR_Cipher_OpenSSL(AES_CTR_Cipher_OpenSSL&& toMove) = default;

                void Reset() override;

            protected:
                size_t GetBlockSizeBytes() const override;

                size_t GetKeyLengthBits() const override;

            private:
                void InitCipher();

                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;
            };

            /**
             * OpenSSL implementation for AES in GCM mode
             */
            class AES_GCM_Cipher_OpenSSL : public OpenSSLCipher
            {
            public:
                /**
                 * Create AES in GCM mode off of a 256 bit key. Auto Generates a 12 byte secure random IV.
                 */
                AES_GCM_Cipher_OpenSSL(const CryptoBuffer& key);

                /**
                * Create AES in GCM mode off of a 256 bit key and AAD. Auto Generates a 12 byte IV in the format
                */
                AES_GCM_Cipher_OpenSSL(const CryptoBuffer& key, const CryptoBuffer* aad);

                /**
                 * Create AES in GCM mode off of a 256 bit key, a 12 byte secure random IV, and an optional 16 byte Tag and additional authentication data (AAD).
                 * Note that tag could be acquired from encrypt mode and should only be set for decrypt mode.
                 * If you are using this cipher to decrypt an encrypted payload, you must set the tag here.
                 */
                AES_GCM_Cipher_OpenSSL(CryptoBuffer&& key, CryptoBuffer&& initializationVector,
                                       CryptoBuffer&& tag = CryptoBuffer(0), CryptoBuffer&& aad = CryptoBuffer(0));

                /**
                 * Create AES in GCM mode off of a 256 bit key, a 12 byte secure random IV, and an optional 16 byte Tag and additional authentication data (AAD).
                 * Note that tag could be acquired from encrypt mode and should only be set for decrypt mode.
                 * If you are using this cipher to decrypt an encrypted payload, you must set the tag here.
                 */
                AES_GCM_Cipher_OpenSSL(const CryptoBuffer& key, const CryptoBuffer& initializationVector,
                                       const CryptoBuffer& tag = CryptoBuffer(0), const CryptoBuffer& aad = CryptoBuffer(0));

                AES_GCM_Cipher_OpenSSL(const AES_GCM_Cipher_OpenSSL& other) = delete;

                AES_GCM_Cipher_OpenSSL& operator=(const AES_GCM_Cipher_OpenSSL& other) = delete;

                AES_GCM_Cipher_OpenSSL(AES_GCM_Cipher_OpenSSL&& toMove) = default;

                /**
                 * Calls base class first, then grabs the tag from the cipher and sets it on m_tag.
                 * After calling FinalizeEncryption, be sure to call GetTag() and do something with it
                 * or you will not be able to decrypt the payload.
                 */
                CryptoBuffer FinalizeEncryption() override;

                void Reset() override;

            protected:
                size_t GetBlockSizeBytes() const override;

                size_t GetKeyLengthBits() const override;

                size_t GetTagLengthBytes() const;

            private:
                void InitCipher();

                CryptoBuffer m_aad;
                static size_t BlockSizeBytes;
                static size_t IVLengthBytes;
                static size_t KeyLengthBits;
                static size_t TagLengthBytes;
            };

            /**
             * OpenSSL implementation for AES in Key Wrap mode. The key for the c_tor is the Kek,
             * it either encrypts a CEK or decrypts it.
             */
            class AES_KeyWrap_Cipher_OpenSSL : public OpenSSLCipher
            {
            public:

                /**
                 * Create AES in Key Wrap mode off of a 256 bit key.
                 */
                AES_KeyWrap_Cipher_OpenSSL(const CryptoBuffer& key);

                AES_KeyWrap_Cipher_OpenSSL(const AES_KeyWrap_Cipher_OpenSSL&) = delete;

                AES_KeyWrap_Cipher_OpenSSL& operator=(const AES_KeyWrap_Cipher_OpenSSL&) = delete;

                AES_KeyWrap_Cipher_OpenSSL(AES_KeyWrap_Cipher_OpenSSL&&) = default;

                CryptoBuffer EncryptBuffer(const CryptoBuffer&) override;
                CryptoBuffer FinalizeEncryption() override;

                CryptoBuffer DecryptBuffer(const CryptoBuffer&) override;
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                inline size_t GetBlockSizeBytes() const override { return BlockSizeBytes; }

                inline size_t GetKeyLengthBits() const override { return KeyLengthBits; }

            private:
                void InitCipher();

                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;

                CryptoBuffer m_workingKeyBuffer;
            };

        } // namespace Crypto
    } // namespace Utils
} // namespace Aws
