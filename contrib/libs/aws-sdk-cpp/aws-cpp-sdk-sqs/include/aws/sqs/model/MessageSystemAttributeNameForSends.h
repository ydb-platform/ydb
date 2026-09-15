/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
namespace SQS
{
namespace Model
{
  enum class MessageSystemAttributeNameForSends
  {
    NOT_SET,
    AWSTraceHeader
  };

namespace MessageSystemAttributeNameForSendsMapper
{
AWS_SQS_API MessageSystemAttributeNameForSends GetMessageSystemAttributeNameForSendsForName(const Aws::String& name);

AWS_SQS_API Aws::String GetNameForMessageSystemAttributeNameForSends(MessageSystemAttributeNameForSends value);
} // namespace MessageSystemAttributeNameForSendsMapper
} // namespace Model
} // namespace SQS
} // namespace Aws
