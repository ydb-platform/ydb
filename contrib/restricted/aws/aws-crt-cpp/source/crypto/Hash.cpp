/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/crt/crypto/Hash.h>

#include <aws/cal/hash.h>

namespace Aws
{
    namespace Crt
    {
        namespace Crypto
        {
            bool ComputeSHA256(
                Allocator *allocator,
                const ByteCursor &input,
                ByteBuf &output,
                size_t truncateTo) noexcept
            {
                return aws_sha256_compute(allocator, &input, &output, truncateTo) == AWS_OP_SUCCESS;
            }

            bool ComputeSHA256(const ByteCursor &input, ByteBuf &output, size_t truncateTo) noexcept
            {
                return aws_sha256_compute(ApiAllocator(), &input, &output, truncateTo) == AWS_OP_SUCCESS;
            }

            bool ComputeMD5(Allocator *allocator, const ByteCursor &input, ByteBuf &output, size_t truncateTo) noexcept
            {
                return aws_md5_compute(allocator, &input, &output, truncateTo) == AWS_OP_SUCCESS;
            }

            bool ComputeMD5(const ByteCursor &input, ByteBuf &output, size_t truncateTo) noexcept
            {
                return aws_md5_compute(ApiAllocator(), &input, &output, truncateTo) == AWS_OP_SUCCESS;
            }

            Hash::Hash(aws_hash *hash) noexcept : m_hash(hash), m_good(false), m_lastError(0)
            {
                if (hash)
                {
                    m_good = true;
                }
                else
                {
                    m_lastError = aws_last_error();
                }
            }

            Hash::~Hash()
            {
                if (m_hash)
                {
                    aws_hash_destroy(m_hash);
                    m_hash = nullptr;
                }
            }

            Hash::Hash(Hash &&toMove) : m_hash(toMove.m_hash), m_good(toMove.m_good), m_lastError(toMove.m_lastError)
            {
                toMove.m_hash = nullptr;
                toMove.m_good = false;
            }

            Hash &Hash::operator=(Hash &&toMove)
            {
                if (&toMove != this)
                {
                    *this = Hash(std::move(toMove));
                }

                return *this;
            }

            Hash Hash::CreateSHA256(Allocator *allocator) noexcept { return Hash(aws_sha256_new(allocator)); }

            Hash Hash::CreateMD5(Allocator *allocator) noexcept { return Hash(aws_md5_new(allocator)); }

            bool Hash::Update(const ByteCursor &toHash) noexcept
            {
                if (*this)
                {
                    if (aws_hash_update(m_hash, &toHash))
                    {
                        m_lastError = aws_last_error();
                        m_good = false;
                        return false;
                    }
                    return true;
                }

                return false;
            }

            bool Hash::Digest(ByteBuf &output, size_t truncateTo) noexcept
            {
                if (*this)
                {
                    m_good = false;
                    if (aws_hash_finalize(m_hash, &output, truncateTo))
                    {
                        m_lastError = aws_last_error();
                        return false;
                    }
                    return true;
                }

                return false;
            }

            aws_hash_vtable ByoHash::s_Vtable = {
                "aws-crt-cpp-byo-crypto-hash",
                "aws-crt-cpp-byo-crypto",
                ByoHash::s_Destroy,
                ByoHash::s_Update,
                ByoHash::s_Finalize,
            };

            ByoHash::ByoHash(size_t digestSize, Allocator *allocator)
            {
                AWS_ZERO_STRUCT(m_hashValue);
                m_hashValue.vtable = &s_Vtable;
                m_hashValue.allocator = allocator;
                m_hashValue.impl = reinterpret_cast<void *>(this);
                m_hashValue.digest_size = digestSize;
                m_hashValue.good = true;
            }

            ByoHash::~ByoHash() {}

            aws_hash *ByoHash::SeatForCInterop(const std::shared_ptr<ByoHash> &selfRef)
            {
                AWS_FATAL_ASSERT(this == selfRef.get());
                m_selfReference = selfRef;
                return &m_hashValue;
            }

            void ByoHash::s_Destroy(struct aws_hash *hash)
            {
                auto *byoHash = reinterpret_cast<ByoHash *>(hash->impl);
                byoHash->m_selfReference = nullptr;
            }

            int ByoHash::s_Update(struct aws_hash *hash, const struct aws_byte_cursor *buf)
            {
                auto *byoHash = reinterpret_cast<ByoHash *>(hash->impl);
                if (!byoHash->m_hashValue.good)
                {
                    return aws_raise_error(AWS_ERROR_INVALID_STATE);
                }
                if (!byoHash->UpdateInternal(*buf))
                {
                    byoHash->m_hashValue.good = false;
                    return AWS_OP_ERR;
                }
                return AWS_OP_SUCCESS;
            }

            int ByoHash::s_Finalize(struct aws_hash *hash, struct aws_byte_buf *out)
            {
                auto *byoHash = reinterpret_cast<ByoHash *>(hash->impl);
                if (!byoHash->m_hashValue.good)
                {
                    return aws_raise_error(AWS_ERROR_INVALID_STATE);
                }

                bool success = byoHash->DigestInternal(*out);
                byoHash->m_hashValue.good = false;
                return success ? AWS_OP_SUCCESS : AWS_OP_ERR;
            }
        } // namespace Crypto
    }     // namespace Crt
} // namespace Aws
