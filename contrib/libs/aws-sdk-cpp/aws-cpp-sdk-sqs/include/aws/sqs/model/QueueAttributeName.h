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
  enum class QueueAttributeName
  {
    NOT_SET,
    All,
    Policy,
    VisibilityTimeout,
    MaximumMessageSize,
    MessageRetentionPeriod,
    ApproximateNumberOfMessages,
    ApproximateNumberOfMessagesNotVisible,
    CreatedTimestamp,
    LastModifiedTimestamp,
    QueueArn,
    ApproximateNumberOfMessagesDelayed,
    DelaySeconds,
    ReceiveMessageWaitTimeSeconds,
    RedrivePolicy,
    FifoQueue,
    ContentBasedDeduplication,
    KmsMasterKeyId,
    KmsDataKeyReusePeriodSeconds,
    DeduplicationScope,
    FifoThroughputLimit,
    RedriveAllowPolicy,
    SqsManagedSseEnabled,
    SentTimestamp,
    ApproximateFirstReceiveTimestamp,
    ApproximateReceiveCount,
    SenderId
  };

namespace QueueAttributeNameMapper
{
AWS_SQS_API QueueAttributeName GetQueueAttributeNameForName(const Aws::String& name);

AWS_SQS_API Aws::String GetNameForQueueAttributeName(QueueAttributeName value);
} // namespace QueueAttributeNameMapper
} // namespace Model
} // namespace SQS
} // namespace Aws
