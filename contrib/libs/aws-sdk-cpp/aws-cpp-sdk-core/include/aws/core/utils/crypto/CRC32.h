/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*
* Interface for CRC32 and CRC32C
*/
#pragma once

#ifdef __APPLE__

#ifdef __clang__
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif // __clang__

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // __GNUC__

#endif // __APPLE__

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/crypto/Hash.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * CRC32 hash implementation.
             */
            class AWS_CORE_API CRC32 : public Hash
            {
            public:
                /**
                 * Initializes platform crypto libs for crc32.
                 */
                CRC32();
                virtual ~CRC32();

                /**
                * Calculates a CRC32 Hash digest
                */
                virtual HashResult Calculate(const Aws::String& str) override;

                /**
                * Calculates a CRC32 Hash digest on a stream (the entire stream is read)
                */
                virtual HashResult Calculate(Aws::IStream& stream) override;

                /**
                 * Updates a Hash digest
                 */
                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                /**
                 * Get the result in the current value
                 */
                virtual HashResult GetHash() override;
            private:

                std::shared_ptr< Hash > m_hashImpl;
            };

            /**
             * CRC32C hash implementation.
             */
            class AWS_CORE_API CRC32C : public Hash
            {
            public:
                /**
                 * Initializes platform crypto libs for crc32c.
                 */
                CRC32C();
                virtual ~CRC32C();

                /**
                * Calculates a CRC32C Hash digest
                */
                virtual HashResult Calculate(const Aws::String& str) override;

                /**
                * Calculates a CRC32C Hash digest on a stream (the entire stream is read)
                */
                virtual HashResult Calculate(Aws::IStream& stream) override;

                /**
                 * Updates a Hash digest
                 */
                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                /**
                 * Get the result in the current value
                 */
                virtual HashResult GetHash() override;

            private:

                std::shared_ptr< Hash > m_hashImpl;
            };

            class AWS_CORE_API CRC32Impl : public Hash
            {
            public:

                CRC32Impl();
                virtual ~CRC32Impl() {}

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                virtual HashResult GetHash() override;

            private:
                int m_runningCrc32;
            };

            class AWS_CORE_API CRC32CImpl : public Hash
            {
            public:

                CRC32CImpl();
                virtual ~CRC32CImpl() {}

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

                virtual void Update(unsigned char* buffer, size_t bufferSize) override;

                virtual HashResult GetHash() override;

            private:
                int m_runningCrc32c;
            };

        } // namespace Crypto
    } // namespace Utils
} // namespace Aws

