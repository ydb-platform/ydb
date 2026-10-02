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
  enum class MessageSystemAttributeName
  {
    NOT_SET,
    SenderId,
    SentTimestamp,
    ApproximateReceiveCount,
    ApproximateFirstReceiveTimestamp,
    SequenceNumber,
    MessageDeduplicationId,
    MessageGroupId,
    AWSTraceHeader
  };

namespace MessageSystemAttributeNameMapper
{
AWS_SQS_API MessageSystemAttributeName GetMessageSystemAttributeNameForName(const Aws::String& name);

AWS_SQS_API Aws::String GetNameForMessageSystemAttributeName(MessageSystemAttributeName value);
} // namespace MessageSystemAttributeNameMapper
} // namespace Model
} // namespace SQS
} // namespace Aws
