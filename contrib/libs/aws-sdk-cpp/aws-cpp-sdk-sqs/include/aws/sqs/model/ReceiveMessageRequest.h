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
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ReceiveMessageRequest">AWS
   * API Reference</a></p>
   */
  class ReceiveMessageRequest : public SQSRequest
  {
  public:
    AWS_SQS_API ReceiveMessageRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ReceiveMessage"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline ReceiveMessageRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline ReceiveMessageRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which messages are received.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline ReceiveMessageRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline const Aws::Vector<QueueAttributeName>& GetAttributeNames() const{ return m_attributeNames; }

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline bool AttributeNamesHasBeenSet() const { return m_attributeNamesHasBeenSet; }

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline void SetAttributeNames(const Aws::Vector<QueueAttributeName>& value) { m_attributeNamesHasBeenSet = true; m_attributeNames = value; }

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline void SetAttributeNames(Aws::Vector<QueueAttributeName>&& value) { m_attributeNamesHasBeenSet = true; m_attributeNames = std::move(value); }

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline ReceiveMessageRequest& WithAttributeNames(const Aws::Vector<QueueAttributeName>& value) { SetAttributeNames(value); return *this;}

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline ReceiveMessageRequest& WithAttributeNames(Aws::Vector<QueueAttributeName>&& value) { SetAttributeNames(std::move(value)); return *this;}

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline ReceiveMessageRequest& AddAttributeNames(const QueueAttributeName& value) { m_attributeNamesHasBeenSet = true; m_attributeNames.push_back(value); return *this; }

    /**
     * <p>A list of attributes that need to be returned along with each message. These
     * attributes include:</p> <ul> <li> <p> <code>All</code> – Returns all values.</p>
     * </li> <li> <p> <code>ApproximateFirstReceiveTimestamp</code> – Returns the time
     * the message was first received from the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>ApproximateReceiveCount</code> – Returns
     * the number of times a message has been received across all queues but not
     * deleted.</p> </li> <li> <p> <code>AWSTraceHeader</code> – Returns the X-Ray
     * trace header string. </p> </li> <li> <p> <code>SenderId</code> </p> <ul> <li>
     * <p>For an IAM user, returns the IAM user ID, for example
     * <code>ABCDEFGHI1JKLMNOPQ23R</code>.</p> </li> <li> <p>For an IAM role, returns
     * the IAM role ID, for example <code>ABCDE1F2GH3I4JK5LMNOP:i-a123b456</code>.</p>
     * </li> </ul> </li> <li> <p> <code>SentTimestamp</code> – Returns the time the
     * message was sent to the queue (<a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds).</p> </li> <li> <p> <code>SqsManagedSseEnabled</code> – Enables
     * server-side queue encryption using SQS owned encryption keys. Only one
     * server-side encryption option is supported per queue (e.g. <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html">SSE-KMS</a>
     * or <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sqs-sse-queue.html">SSE-SQS</a>).</p>
     * </li> <li> <p> <code>MessageDeduplicationId</code> – Returns the value provided
     * by the producer that calls the <code> <a>SendMessage</a> </code> action.</p>
     * </li> <li> <p> <code>MessageGroupId</code> – Returns the value provided by the
     * producer that calls the <code> <a>SendMessage</a> </code> action. Messages with
     * the same <code>MessageGroupId</code> are returned in sequence.</p> </li> <li>
     * <p> <code>SequenceNumber</code> – Returns the value provided by Amazon SQS.</p>
     * </li> </ul>
     */
    inline ReceiveMessageRequest& AddAttributeNames(QueueAttributeName&& value) { m_attributeNamesHasBeenSet = true; m_attributeNames.push_back(std::move(value)); return *this; }


    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline const Aws::Vector<Aws::String>& GetMessageAttributeNames() const{ return m_messageAttributeNames; }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline bool MessageAttributeNamesHasBeenSet() const { return m_messageAttributeNamesHasBeenSet; }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline void SetMessageAttributeNames(const Aws::Vector<Aws::String>& value) { m_messageAttributeNamesHasBeenSet = true; m_messageAttributeNames = value; }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline void SetMessageAttributeNames(Aws::Vector<Aws::String>&& value) { m_messageAttributeNamesHasBeenSet = true; m_messageAttributeNames = std::move(value); }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline ReceiveMessageRequest& WithMessageAttributeNames(const Aws::Vector<Aws::String>& value) { SetMessageAttributeNames(value); return *this;}

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline ReceiveMessageRequest& WithMessageAttributeNames(Aws::Vector<Aws::String>&& value) { SetMessageAttributeNames(std::move(value)); return *this;}

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline ReceiveMessageRequest& AddMessageAttributeNames(const Aws::String& value) { m_messageAttributeNamesHasBeenSet = true; m_messageAttributeNames.push_back(value); return *this; }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline ReceiveMessageRequest& AddMessageAttributeNames(Aws::String&& value) { m_messageAttributeNamesHasBeenSet = true; m_messageAttributeNames.push_back(std::move(value)); return *this; }

    /**
     * <p>The name of the message attribute, where <i>N</i> is the index.</p> <ul> <li>
     * <p>The name can contain alphanumeric characters and the underscore
     * (<code>_</code>), hyphen (<code>-</code>), and period (<code>.</code>).</p>
     * </li> <li> <p>The name is case-sensitive and must be unique among all attribute
     * names for the message.</p> </li> <li> <p>The name must not start with
     * AWS-reserved prefixes such as <code>AWS.</code> or <code>Amazon.</code> (or any
     * casing variants).</p> </li> <li> <p>The name must not start or end with a period
     * (<code>.</code>), and it should not have periods in succession
     * (<code>..</code>).</p> </li> <li> <p>The name can be up to 256 characters
     * long.</p> </li> </ul> <p>When using <code>ReceiveMessage</code>, you can send a
     * list of attribute names to receive, or you can return all of the attributes by
     * specifying <code>All</code> or <code>.*</code> in your request. You can also use
     * all message attributes starting with a prefix, for example
     * <code>bar.*</code>.</p>
     */
    inline ReceiveMessageRequest& AddMessageAttributeNames(const char* value) { m_messageAttributeNamesHasBeenSet = true; m_messageAttributeNames.push_back(value); return *this; }


    /**
     * <p>The maximum number of messages to return. Amazon SQS never returns more
     * messages than this value (however, fewer messages might be returned). Valid
     * values: 1 to 10. Default: 1.</p>
     */
    inline int GetMaxNumberOfMessages() const{ return m_maxNumberOfMessages; }

    /**
     * <p>The maximum number of messages to return. Amazon SQS never returns more
     * messages than this value (however, fewer messages might be returned). Valid
     * values: 1 to 10. Default: 1.</p>
     */
    inline bool MaxNumberOfMessagesHasBeenSet() const { return m_maxNumberOfMessagesHasBeenSet; }

    /**
     * <p>The maximum number of messages to return. Amazon SQS never returns more
     * messages than this value (however, fewer messages might be returned). Valid
     * values: 1 to 10. Default: 1.</p>
     */
    inline void SetMaxNumberOfMessages(int value) { m_maxNumberOfMessagesHasBeenSet = true; m_maxNumberOfMessages = value; }

    /**
     * <p>The maximum number of messages to return. Amazon SQS never returns more
     * messages than this value (however, fewer messages might be returned). Valid
     * values: 1 to 10. Default: 1.</p>
     */
    inline ReceiveMessageRequest& WithMaxNumberOfMessages(int value) { SetMaxNumberOfMessages(value); return *this;}


    /**
     * <p>The duration (in seconds) that the received messages are hidden from
     * subsequent retrieve requests after being retrieved by a
     * <code>ReceiveMessage</code> request.</p>
     */
    inline int GetVisibilityTimeout() const{ return m_visibilityTimeout; }

    /**
     * <p>The duration (in seconds) that the received messages are hidden from
     * subsequent retrieve requests after being retrieved by a
     * <code>ReceiveMessage</code> request.</p>
     */
    inline bool VisibilityTimeoutHasBeenSet() const { return m_visibilityTimeoutHasBeenSet; }

    /**
     * <p>The duration (in seconds) that the received messages are hidden from
     * subsequent retrieve requests after being retrieved by a
     * <code>ReceiveMessage</code> request.</p>
     */
    inline void SetVisibilityTimeout(int value) { m_visibilityTimeoutHasBeenSet = true; m_visibilityTimeout = value; }

    /**
     * <p>The duration (in seconds) that the received messages are hidden from
     * subsequent retrieve requests after being retrieved by a
     * <code>ReceiveMessage</code> request.</p>
     */
    inline ReceiveMessageRequest& WithVisibilityTimeout(int value) { SetVisibilityTimeout(value); return *this;}


    /**
     * <p>The duration (in seconds) for which the call waits for a message to arrive in
     * the queue before returning. If a message is available, the call returns sooner
     * than <code>WaitTimeSeconds</code>. If no messages are available and the wait
     * time expires, the call returns successfully with an empty list of messages.</p>
     *  <p>To avoid HTTP errors, ensure that the HTTP response timeout for
     * <code>ReceiveMessage</code> requests is longer than the
     * <code>WaitTimeSeconds</code> parameter. For example, with the Java SDK, you can
     * set HTTP transport settings using the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.html">
     * NettyNioAsyncHttpClient</a> for asynchronous clients, or the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.html">
     * ApacheHttpClient</a> for synchronous clients. </p> 
     */
    inline int GetWaitTimeSeconds() const{ return m_waitTimeSeconds; }

    /**
     * <p>The duration (in seconds) for which the call waits for a message to arrive in
     * the queue before returning. If a message is available, the call returns sooner
     * than <code>WaitTimeSeconds</code>. If no messages are available and the wait
     * time expires, the call returns successfully with an empty list of messages.</p>
     *  <p>To avoid HTTP errors, ensure that the HTTP response timeout for
     * <code>ReceiveMessage</code> requests is longer than the
     * <code>WaitTimeSeconds</code> parameter. For example, with the Java SDK, you can
     * set HTTP transport settings using the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.html">
     * NettyNioAsyncHttpClient</a> for asynchronous clients, or the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.html">
     * ApacheHttpClient</a> for synchronous clients. </p> 
     */
    inline bool WaitTimeSecondsHasBeenSet() const { return m_waitTimeSecondsHasBeenSet; }

    /**
     * <p>The duration (in seconds) for which the call waits for a message to arrive in
     * the queue before returning. If a message is available, the call returns sooner
     * than <code>WaitTimeSeconds</code>. If no messages are available and the wait
     * time expires, the call returns successfully with an empty list of messages.</p>
     *  <p>To avoid HTTP errors, ensure that the HTTP response timeout for
     * <code>ReceiveMessage</code> requests is longer than the
     * <code>WaitTimeSeconds</code> parameter. For example, with the Java SDK, you can
     * set HTTP transport settings using the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.html">
     * NettyNioAsyncHttpClient</a> for asynchronous clients, or the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.html">
     * ApacheHttpClient</a> for synchronous clients. </p> 
     */
    inline void SetWaitTimeSeconds(int value) { m_waitTimeSecondsHasBeenSet = true; m_waitTimeSeconds = value; }

    /**
     * <p>The duration (in seconds) for which the call waits for a message to arrive in
     * the queue before returning. If a message is available, the call returns sooner
     * than <code>WaitTimeSeconds</code>. If no messages are available and the wait
     * time expires, the call returns successfully with an empty list of messages.</p>
     *  <p>To avoid HTTP errors, ensure that the HTTP response timeout for
     * <code>ReceiveMessage</code> requests is longer than the
     * <code>WaitTimeSeconds</code> parameter. For example, with the Java SDK, you can
     * set HTTP transport settings using the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.html">
     * NettyNioAsyncHttpClient</a> for asynchronous clients, or the <a
     * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.html">
     * ApacheHttpClient</a> for synchronous clients. </p> 
     */
    inline ReceiveMessageRequest& WithWaitTimeSeconds(int value) { SetWaitTimeSeconds(value); return *this;}


    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline const Aws::String& GetReceiveRequestAttemptId() const{ return m_receiveRequestAttemptId; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline bool ReceiveRequestAttemptIdHasBeenSet() const { return m_receiveRequestAttemptIdHasBeenSet; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline void SetReceiveRequestAttemptId(const Aws::String& value) { m_receiveRequestAttemptIdHasBeenSet = true; m_receiveRequestAttemptId = value; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline void SetReceiveRequestAttemptId(Aws::String&& value) { m_receiveRequestAttemptIdHasBeenSet = true; m_receiveRequestAttemptId = std::move(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline void SetReceiveRequestAttemptId(const char* value) { m_receiveRequestAttemptIdHasBeenSet = true; m_receiveRequestAttemptId.assign(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline ReceiveMessageRequest& WithReceiveRequestAttemptId(const Aws::String& value) { SetReceiveRequestAttemptId(value); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline ReceiveMessageRequest& WithReceiveRequestAttemptId(Aws::String&& value) { SetReceiveRequestAttemptId(std::move(value)); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * token used for deduplication of <code>ReceiveMessage</code> calls. If a
     * networking issue occurs after a <code>ReceiveMessage</code> action, and instead
     * of a response you receive a generic error, it is possible to retry the same
     * action with an identical <code>ReceiveRequestAttemptId</code> to retrieve the
     * same set of messages, even if their visibility timeout has not yet expired.</p>
     * <ul> <li> <p>You can use <code>ReceiveRequestAttemptId</code> only for 5 minutes
     * after a <code>ReceiveMessage</code> action.</p> </li> <li> <p>When you set
     * <code>FifoQueue</code>, a caller of the <code>ReceiveMessage</code> action can
     * provide a <code>ReceiveRequestAttemptId</code> explicitly.</p> </li> <li> <p>If
     * a caller of the <code>ReceiveMessage</code> action doesn't provide a
     * <code>ReceiveRequestAttemptId</code>, Amazon SQS generates a
     * <code>ReceiveRequestAttemptId</code>.</p> </li> <li> <p>It is possible to retry
     * the <code>ReceiveMessage</code> action with the same
     * <code>ReceiveRequestAttemptId</code> if none of the messages have been modified
     * (deleted or had their visibility changes).</p> </li> <li> <p>During a visibility
     * timeout, subsequent calls with the same <code>ReceiveRequestAttemptId</code>
     * return the same messages and receipt handles. If a retry occurs within the
     * deduplication interval, it resets the visibility timeout. For more information,
     * see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html">Visibility
     * Timeout</a> in the <i>Amazon SQS Developer Guide</i>.</p>  <p>If a
     * caller of the <code>ReceiveMessage</code> action still processes messages when
     * the visibility timeout expires and messages become visible, another worker
     * consuming from the same queue can receive the same messages and therefore
     * process duplicates. Also, if a consumer whose message processing time is longer
     * than the visibility timeout tries to delete the processed messages, the action
     * fails with an error.</p> <p>To mitigate this effect, ensure that your
     * application observes a safe threshold before the visibility timeout expires and
     * extend the visibility timeout as necessary.</p>  </li> <li> <p>While
     * messages with a particular <code>MessageGroupId</code> are invisible, no more
     * messages belonging to the same <code>MessageGroupId</code> are returned until
     * the visibility timeout expires. You can still receive messages with another
     * <code>MessageGroupId</code> as long as it is also visible.</p> </li> <li> <p>If
     * a caller of <code>ReceiveMessage</code> can't track the
     * <code>ReceiveRequestAttemptId</code>, no retries work until the original
     * visibility timeout expires. As a result, delays might occur but the messages in
     * the queue remain in a strict order.</p> </li> </ul> <p>The maximum length of
     * <code>ReceiveRequestAttemptId</code> is 128 characters.
     * <code>ReceiveRequestAttemptId</code> can contain alphanumeric characters
     * (<code>a-z</code>, <code>A-Z</code>, <code>0-9</code>) and punctuation
     * (<code>!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~</code>).</p> <p>For best
     * practices of using <code>ReceiveRequestAttemptId</code>, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-receiverequestattemptid-request-parameter.html">Using
     * the ReceiveRequestAttemptId Request Parameter</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline ReceiveMessageRequest& WithReceiveRequestAttemptId(const char* value) { SetReceiveRequestAttemptId(value); return *this;}

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;

    Aws::Vector<QueueAttributeName> m_attributeNames;
    bool m_attributeNamesHasBeenSet = false;

    Aws::Vector<Aws::String> m_messageAttributeNames;
    bool m_messageAttributeNamesHasBeenSet = false;

    int m_maxNumberOfMessages;
    bool m_maxNumberOfMessagesHasBeenSet = false;

    int m_visibilityTimeout;
    bool m_visibilityTimeoutHasBeenSet = false;

    int m_waitTimeSeconds;
    bool m_waitTimeSecondsHasBeenSet = false;

    Aws::String m_receiveRequestAttemptId;
    bool m_receiveRequestAttemptIdHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
