/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <memory>

namespace Aws
{
    namespace Client
    {
        class AWSAuthSigner;
    }
    namespace Auth
    {
        class AWSCredentialsProvider;

        class AWS_CORE_API AWSAuthSignerProvider
        {
        public:
            virtual std::shared_ptr<Aws::Client::AWSAuthSigner> GetSigner(const Aws::String& signerName) const = 0;
            virtual void AddSigner(std::shared_ptr<Aws::Client::AWSAuthSigner>& signer) = 0;
            virtual ~AWSAuthSignerProvider() = default;
        };
    }
}
