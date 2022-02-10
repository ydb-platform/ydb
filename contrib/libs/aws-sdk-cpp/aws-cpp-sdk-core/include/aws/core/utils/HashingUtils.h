/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/Array.h>

namespace Aws
{
    namespace Utils
    {

        /**
        * Generic utils for hashing strings
        */
        class AWS_CORE_API HashingUtils
        {
        public:
            /**
            * Base64 encodes string
            */
            static Aws::String Base64Encode(const ByteBuffer& byteBuffer);

            /**
            * Base64 decodes string
            */
            static ByteBuffer Base64Decode(const Aws::String&);

            /**
            * Hex encodes string
            */
            static Aws::String HexEncode(const ByteBuffer& byteBuffer);

            /**
            * Hex encodes string
            */
            static ByteBuffer HexDecode(const Aws::String& str);

            /**
            * Calculates a SHA256 HMAC digest (not hex encoded)
            */
            static ByteBuffer CalculateSHA256HMAC(const ByteBuffer& toSign, const ByteBuffer& secret);

            /**
            * Calculates a SHA256 Hash digest (not hex encoded)
            */
            static ByteBuffer CalculateSHA256(const Aws::String& str);

            /**
            * Calculates a SHA256 Hash digest on a stream (the entire stream is read, not hex encoded.)
            */
            static ByteBuffer CalculateSHA256(Aws::IOStream& stream);

            /**
            * Calculates a SHA256 Tree Hash digest (not hex encoded, see tree hash definition: http://docs.aws.amazon.com/amazonglacier/latest/dev/checksum-calculations.html)
            */
            static ByteBuffer CalculateSHA256TreeHash(const Aws::String& str);

            /**
            * Calculates a SHA256 Tree Hash digest on a stream (the entire stream is read, not hex encoded.)
            */
            static ByteBuffer CalculateSHA256TreeHash(Aws::IOStream& stream);

            /**
            * Calculates a MD5 Hash value
            */
            static ByteBuffer CalculateMD5(const Aws::String& str);

            /**
            * Calculates a MD5 Hash value
            */
            static ByteBuffer CalculateMD5(Aws::IOStream& stream);

            static int HashString(const char* strToHash);

        };

    } // namespace Utils
} // namespace Aws

