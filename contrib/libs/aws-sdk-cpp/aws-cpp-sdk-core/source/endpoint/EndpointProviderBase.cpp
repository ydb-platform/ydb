/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/endpoint/EndpointProviderBase.h>

namespace Aws
{
namespace Endpoint
{
/**
 * Export endpoint provider symbols from DLL
 */
template class AWS_CORE_API EndpointProviderBase<Aws::Client::GenericClientConfiguration<false>,
            Aws::Endpoint::BuiltInParameters,
            Aws::Endpoint::ClientContextParameters>;

} // namespace Endpoint
} // namespace Aws
