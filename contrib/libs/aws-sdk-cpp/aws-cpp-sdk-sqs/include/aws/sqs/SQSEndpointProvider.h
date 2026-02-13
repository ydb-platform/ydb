/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/client/GenericClientConfiguration.h>
#include <aws/core/endpoint/DefaultEndpointProvider.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>

#include <aws/sqs/SQSEndpointRules.h>


namespace Aws
{
namespace SQS
{
namespace Endpoint
{
using EndpointParameters = Aws::Endpoint::EndpointParameters;
using Aws::Endpoint::EndpointProviderBase;
using Aws::Endpoint::DefaultEndpointProvider;

using SQSClientContextParameters = Aws::Endpoint::ClientContextParameters;

using SQSClientConfiguration = Aws::Client::GenericClientConfiguration<false>;
using SQSBuiltInParameters = Aws::Endpoint::BuiltInParameters;

/**
 * The type for the SQS Client Endpoint Provider.
 * Inherit from this Base class / "Interface" should you want to provide a custom endpoint provider.
 * The SDK must use service-specific type for each service per specification.
 */
using SQSEndpointProviderBase =
    EndpointProviderBase<SQSClientConfiguration, SQSBuiltInParameters, SQSClientContextParameters>;

using SQSDefaultEpProviderBase =
    DefaultEndpointProvider<SQSClientConfiguration, SQSBuiltInParameters, SQSClientContextParameters>;

/**
 * Default endpoint provider used for this service
 */
class AWS_SQS_API SQSEndpointProvider : public SQSDefaultEpProviderBase
{
public:
    using SQSResolveEndpointOutcome = Aws::Endpoint::ResolveEndpointOutcome;

    SQSEndpointProvider()
      : SQSDefaultEpProviderBase(Aws::SQS::SQSEndpointRules::GetRulesBlob(), Aws::SQS::SQSEndpointRules::RulesBlobSize)
    {}

    ~SQSEndpointProvider()
    {
    }
};
} // namespace Endpoint
} // namespace SQS
} // namespace Aws
