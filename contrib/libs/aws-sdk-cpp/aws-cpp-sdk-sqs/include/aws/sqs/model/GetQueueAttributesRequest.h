/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/sqs/SQSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/sqs/model/QueueAttributeName.h>
#include <utility>

namespace Aws
{
namespace SQS
{
namespace Model
{

  /**
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/GetQueueAttributesRequest">AWS
   * API Reference</a></p>
   */
  class GetQueueAttributesRequest : public SQSRequest
  {
  public:
    AWS_SQS_API GetQueueAttributesRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetQueueAttributes"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline GetQueueAttributesRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline GetQueueAttributesRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue whose attribute information is retrieved.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline GetQueueAttributesRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline const Aws::Vector<QueueAttributeName>& GetAttributeNames() const{ return m_attributeNames; }

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline bool AttributeNamesHasBeenSet() const { return m_attributeNamesHasBeenSet; }

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline void SetAttributeNames(const Aws::Vector<QueueAttributeName>& value) { m_attributeNamesHasBeenSet = true; m_attributeNames = value; }

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline void SetAttributeNames(Aws::Vector<QueueAttributeName>&& value) { m_attributeNamesHasBeenSet = true; m_attributeNames = std::move(value); }

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline GetQueueAttributesRequest& WithAttributeNames(const Aws::Vector<QueueAttributeName>& value) { SetAttributeNames(value); return *this;}

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline GetQueueAttributesRequest& WithAttributeNames(Aws::Vector<QueueAttributeName>&& value) { SetAttributeNames(std::move(value)); return *this;}

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline GetQueueAttributesRequest& AddAttributeNames(const QueueAttributeName& value) { m_attributeNamesHasBeenSet = true; m_attributeNames.push_back(value); return *this; }

    /**
     * <p>A list of attributes for which to retrieve information.</p> <p>The
     * <code>AttributeName.N</code> parameter is optional, but if you don't specify
     * values for this parameter, the request returns empty results.</p>  <p>In
     * the future, new attributes might be added. If you write code that calls this
     * action, we recommend that you structure your code so that it can handle new
     * attributes gracefully.</p>  <p>The following attributes are
     * supported:</p>  <p>The
     * <code>ApproximateNumberOfMessagesDelayed</code>,
     * <code>ApproximateNumberOfMessagesNotVisible</code>, and
     * <code>ApproximateNumberOfMessagesVisible</code> metrics may not achieve
     * consistency until at least 1 minute after the producers stop sending messages.
     * This period is required for the queue metadata to reach eventual consistency.
     * </p>  <ul> <li> <p> <code>All</code> – Returns all values. </p>
     * </li> <li> <p> <code>ApproximateNumberOfMessages</code> – Returns the
     * approximate number of messages available for retrieval from the queue.</p> </li>
     * <li> <p> <code>ApproximateNumberOfMessagesDelayed</code> – Returns the
     * approximate number of messages in the queue that are delayed and not available
     * for reading immediately. This can happen when the queue is configured as a delay
     * queue or when a message has been sent with a delay parameter.</p> </li> <li> <p>
     * <code>ApproximateNumberOfMessagesNotVisible</code> – Returns the approximate
     * number of messages that are in flight. Messages are considered to be <i>in
     * flight</i> if they have been sent to a client but have not yet been deleted or
     * have not yet reached the end of their visibility window. </p> </li> <li> <p>
     * <code>CreatedTimestamp</code> – Returns the time when the queue was created in
     * seconds (<a href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p>
     * </li> <li> <p> <code>DelaySeconds</code> – Returns the default delay on the
     * queue in seconds.</p> </li> <li> <p> <code>LastModifiedTimestamp</code> –
     * Returns the time when the queue was last changed in seconds (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a>).</p> </li> <li>
     * <p> <code>MaximumMessageSize</code> – Returns the limit of how many bytes a
     * message can contain before Amazon SQS rejects it.</p> </li> <li> <p>
     * <code>MessageRetentionPeriod</code> – Returns the length of time, in seconds,
     * for which Amazon SQS retains a message.</p> </li> <li> <p> <code>Policy</code> –
     * Returns the policy of the queue.</p> </li> <li> <p> <code>QueueArn</code> –
     * Returns the Amazon resource name (ARN) of the queue.</p> </li> <li> <p>
     * <code>ReceiveMessageWaitTimeSeconds</code> – Returns the length of time, in
     * seconds, for which the <code>ReceiveMessage</code> action waits for a message to
     * arrive. </p> </li> <li> <p> <code>RedrivePolicy</code> – The string that
     * includes the parameters for the dead-letter queue functionality of the source
     * queue as a JSON object. For more information about the redrive policy and
     * dead-letter queues, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html">Using
     * Amazon SQS Dead-Letter Queues</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     * <ul> <li> <p> <code>deadLetterTargetArn</code> – The Amazon Resource Name (ARN)
     * of the dead-letter queue to which Amazon SQS moves messages after the value of
     * <code>maxReceiveCount</code> is exceeded.</p> </li> <li> <p>
     * <code>maxReceiveCount</code> – The number of times a message is delivered to the
     * source queue before being moved to the dead-letter queue. When the
     * <code>ReceiveCount</code> for a message exceeds the <code>maxReceiveCount</code>
     * for a queue, Amazon SQS moves the message to the dead-letter-queue.</p> </li>
     * </ul> </li> <li> <p> <code>VisibilityTimeout</code> – Returns the visibility
     * timeout for the queue. For more information about the visibility timeout, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html">server-side-encryption</a>:</p>
     * <ul> <li> <p> <code>KmsMasterKeyId</code> – Returns the ID of an Amazon Web
     * Services managed customer master key (CMK) for Amazon SQS or a custom CMK. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-sse-key-terms">Key
     * Terms</a>. </p> </li> <li> <p> <code>KmsDataKeyReusePeriodSeconds</code> –
     * Returns the length of time, in seconds, for which Amazon SQS can reuse a data
     * key to encrypt or decrypt messages before calling KMS again. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html#sqs-how-does-the-data-key-reuse-period-work">How
     * Does the Data Key Reuse Period Work?</a>. </p> </li> <li> <p>
     * <code>SqsManagedSseEnabled</code> – Returns information about whether the queue
     * is using SSE-SQS encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> </ul> <p>The following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO
     * (first-in-first-out) queues</a>:</p> <ul> <li> <p> <code>FifoQueue</code> –
     * Returns information about whether the queue is FIFO. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html">FIFO
     * queue logic</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>To
     * determine whether a queue is <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">FIFO</a>,
     * you can check whether <code>QueueName</code> ends with the <code>.fifo</code>
     * suffix.</p>  </li> <li> <p> <code>ContentBasedDeduplication</code> –
     * Returns whether content-based deduplication is enabled for the queue. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html">Exactly-once
     * processing</a> in the <i>Amazon SQS Developer Guide</i>. </p> </li> </ul> <p>The
     * following attributes apply only to <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html">high
     * throughput for FIFO queues</a>:</p> <ul> <li> <p>
     * <code>DeduplicationScope</code> – Specifies whether message deduplication occurs
     * at the message group or queue level. Valid values are <code>messageGroup</code>
     * and <code>queue</code>.</p> </li> <li> <p> <code>FifoThroughputLimit</code> –
     * Specifies whether the FIFO queue throughput quota applies to the entire queue or
     * per message group. Valid values are <code>perQueue</code> and
     * <code>perMessageGroupId</code>. The <code>perMessageGroupId</code> value is
     * allowed only when the value for <code>DeduplicationScope</code> is
     * <code>messageGroup</code>.</p> </li> </ul> <p>To enable high throughput for FIFO
     * queues, do the following:</p> <ul> <li> <p>Set <code>DeduplicationScope</code>
     * to <code>messageGroup</code>.</p> </li> <li> <p>Set
     * <code>FifoThroughputLimit</code> to <code>perMessageGroupId</code>.</p> </li>
     * </ul> <p>If you set these attributes to anything other than the values shown for
     * enabling high throughput, normal throughput is in effect and deduplication
     * occurs as specified.</p> <p>For information on throughput quotas, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html">Quotas
     * related to messages</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline GetQueueAttributesRequest& AddAttributeNames(QueueAttributeName&& value) { m_attributeNamesHasBeenSet = true; m_attributeNames.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;

    Aws::Vector<QueueAttributeName> m_attributeNames;
    bool m_attributeNamesHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
