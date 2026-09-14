/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/sqs/model/MessageSystemAttributeName.h>
#include <aws/sqs/model/MessageAttributeValue.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace SQS
{
namespace Model
{

  /**
   * <p>An Amazon SQS message.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/Message">AWS API
   * Reference</a></p>
   */
  class Message
  {
  public:
    AWS_SQS_API Message();
    AWS_SQS_API Message(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_SQS_API Message& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_SQS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_SQS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline const Aws::String& GetMessageId() const{ return m_messageId; }

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline bool MessageIdHasBeenSet() const { return m_messageIdHasBeenSet; }

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline void SetMessageId(const Aws::String& value) { m_messageIdHasBeenSet = true; m_messageId = value; }

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline void SetMessageId(Aws::String&& value) { m_messageIdHasBeenSet = true; m_messageId = std::move(value); }

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline void SetMessageId(const char* value) { m_messageIdHasBeenSet = true; m_messageId.assign(value); }

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline Message& WithMessageId(const Aws::String& value) { SetMessageId(value); return *this;}

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline Message& WithMessageId(Aws::String&& value) { SetMessageId(std::move(value)); return *this;}

    /**
     * <p>A unique identifier for the message. A <code>MessageId</code>is considered
     * unique across all Amazon Web Services accounts for an extended period of
     * time.</p>
     */
    inline Message& WithMessageId(const char* value) { SetMessageId(value); return *this;}


    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline const Aws::String& GetReceiptHandle() const{ return m_receiptHandle; }

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline bool ReceiptHandleHasBeenSet() const { return m_receiptHandleHasBeenSet; }

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline void SetReceiptHandle(const Aws::String& value) { m_receiptHandleHasBeenSet = true; m_receiptHandle = value; }

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline void SetReceiptHandle(Aws::String&& value) { m_receiptHandleHasBeenSet = true; m_receiptHandle = std::move(value); }

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline void SetReceiptHandle(const char* value) { m_receiptHandleHasBeenSet = true; m_receiptHandle.assign(value); }

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline Message& WithReceiptHandle(const Aws::String& value) { SetReceiptHandle(value); return *this;}

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline Message& WithReceiptHandle(Aws::String&& value) { SetReceiptHandle(std::move(value)); return *this;}

    /**
     * <p>An identifier associated with the act of receiving the message. A new receipt
     * handle is returned every time you receive a message. When deleting a message,
     * you provide the last received receipt handle to delete the message.</p>
     */
    inline Message& WithReceiptHandle(const char* value) { SetReceiptHandle(value); return *this;}


    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline const Aws::String& GetMD5OfBody() const{ return m_mD5OfBody; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline bool MD5OfBodyHasBeenSet() const { return m_mD5OfBodyHasBeenSet; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline void SetMD5OfBody(const Aws::String& value) { m_mD5OfBodyHasBeenSet = true; m_mD5OfBody = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline void SetMD5OfBody(Aws::String&& value) { m_mD5OfBodyHasBeenSet = true; m_mD5OfBody = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline void SetMD5OfBody(const char* value) { m_mD5OfBodyHasBeenSet = true; m_mD5OfBody.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline Message& WithMD5OfBody(const Aws::String& value) { SetMD5OfBody(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline Message& WithMD5OfBody(Aws::String&& value) { SetMD5OfBody(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string.</p>
     */
    inline Message& WithMD5OfBody(const char* value) { SetMD5OfBody(value); return *this;}


    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline const Aws::String& GetBody() const{ return m_body; }

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline bool BodyHasBeenSet() const { return m_bodyHasBeenSet; }

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline void SetBody(const Aws::String& value) { m_bodyHasBeenSet = true; m_body = value; }

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline void SetBody(Aws::String&& value) { m_bodyHasBeenSet = true; m_body = std::move(value); }

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline void SetBody(const char* value) { m_bodyHasBeenSet = true; m_body.assign(value); }

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline Message& WithBody(const Aws::String& value) { SetBody(value); return *this;}

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline Message& WithBody(Aws::String&& value) { SetBody(std::move(value)); return *this;}

    /**
     * <p>The message's contents (not URL-encoded).</p>
     */
    inline Message& WithBody(const char* value) { SetBody(value); return *this;}


    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline const Aws::Map<MessageSystemAttributeName, Aws::String>& GetAttributes() const{ return m_attributes; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline bool AttributesHasBeenSet() const { return m_attributesHasBeenSet; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline void SetAttributes(const Aws::Map<MessageSystemAttributeName, Aws::String>& value) { m_attributesHasBeenSet = true; m_attributes = value; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline void SetAttributes(Aws::Map<MessageSystemAttributeName, Aws::String>&& value) { m_attributesHasBeenSet = true; m_attributes = std::move(value); }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& WithAttributes(const Aws::Map<MessageSystemAttributeName, Aws::String>& value) { SetAttributes(value); return *this;}

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& WithAttributes(Aws::Map<MessageSystemAttributeName, Aws::String>&& value) { SetAttributes(std::move(value)); return *this;}

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(const MessageSystemAttributeName& key, const Aws::String& value) { m_attributesHasBeenSet = true; m_attributes.emplace(key, value); return *this; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(MessageSystemAttributeName&& key, const Aws::String& value) { m_attributesHasBeenSet = true; m_attributes.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(const MessageSystemAttributeName& key, Aws::String&& value) { m_attributesHasBeenSet = true; m_attributes.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(MessageSystemAttributeName&& key, Aws::String&& value) { m_attributesHasBeenSet = true; m_attributes.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(MessageSystemAttributeName&& key, const char* value) { m_attributesHasBeenSet = true; m_attributes.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of the attributes requested in <code> <a>ReceiveMessage</a> </code> to
     * their respective values. Supported attributes:</p> <ul> <li> <p>
     * <code>ApproximateReceiveCount</code> </p> </li> <li> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> </p> </li> <li> <p>
     * <code>MessageDeduplicationId</code> </p> </li> <li> <p>
     * <code>MessageGroupId</code> </p> </li> <li> <p> <code>SenderId</code> </p> </li>
     * <li> <p> <code>SentTimestamp</code> </p> </li> <li> <p>
     * <code>SequenceNumber</code> </p> </li> </ul> <p>
     * <code>ApproximateFirstReceiveTimestamp</code> and <code>SentTimestamp</code> are
     * each returned as an integer representing the <a
     * href="http://en.wikipedia.org/wiki/Unix_time">epoch time</a> in
     * milliseconds.</p>
     */
    inline Message& AddAttributes(const MessageSystemAttributeName& key, const char* value) { m_attributesHasBeenSet = true; m_attributes.emplace(key, value); return *this; }


    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline const Aws::String& GetMD5OfMessageAttributes() const{ return m_mD5OfMessageAttributes; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline bool MD5OfMessageAttributesHasBeenSet() const { return m_mD5OfMessageAttributesHasBeenSet; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageAttributes(const Aws::String& value) { m_mD5OfMessageAttributesHasBeenSet = true; m_mD5OfMessageAttributes = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageAttributes(Aws::String&& value) { m_mD5OfMessageAttributesHasBeenSet = true; m_mD5OfMessageAttributes = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageAttributes(const char* value) { m_mD5OfMessageAttributesHasBeenSet = true; m_mD5OfMessageAttributes.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline Message& WithMD5OfMessageAttributes(const Aws::String& value) { SetMD5OfMessageAttributes(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline Message& WithMD5OfMessageAttributes(Aws::String&& value) { SetMD5OfMessageAttributes(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline Message& WithMD5OfMessageAttributes(const char* value) { SetMD5OfMessageAttributes(value); return *this;}


    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline const Aws::Map<Aws::String, MessageAttributeValue>& GetMessageAttributes() const{ return m_messageAttributes; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline bool MessageAttributesHasBeenSet() const { return m_messageAttributesHasBeenSet; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline void SetMessageAttributes(const Aws::Map<Aws::String, MessageAttributeValue>& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes = value; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline void SetMessageAttributes(Aws::Map<Aws::String, MessageAttributeValue>&& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes = std::move(value); }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& WithMessageAttributes(const Aws::Map<Aws::String, MessageAttributeValue>& value) { SetMessageAttributes(value); return *this;}

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& WithMessageAttributes(Aws::Map<Aws::String, MessageAttributeValue>&& value) { SetMessageAttributes(std::move(value)); return *this;}

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(const Aws::String& key, const MessageAttributeValue& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(key, value); return *this; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(Aws::String&& key, const MessageAttributeValue& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(std::move(key), value); return *this; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(const Aws::String& key, MessageAttributeValue&& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(key, std::move(value)); return *this; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(Aws::String&& key, MessageAttributeValue&& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(const char* key, MessageAttributeValue&& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(key, std::move(value)); return *this; }

    /**
     * <p>Each message attribute consists of a <code>Name</code>, <code>Type</code>,
     * and <code>Value</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">Amazon
     * SQS message attributes</a> in the <i>Amazon SQS Developer Guide</i>.</p>
     */
    inline Message& AddMessageAttributes(const char* key, const MessageAttributeValue& value) { m_messageAttributesHasBeenSet = true; m_messageAttributes.emplace(key, value); return *this; }

  private:

    Aws::String m_messageId;
    bool m_messageIdHasBeenSet = false;

    Aws::String m_receiptHandle;
    bool m_receiptHandleHasBeenSet = false;

    Aws::String m_mD5OfBody;
    bool m_mD5OfBodyHasBeenSet = false;

    Aws::String m_body;
    bool m_bodyHasBeenSet = false;

    Aws::Map<MessageSystemAttributeName, Aws::String> m_attributes;
    bool m_attributesHasBeenSet = false;

    Aws::String m_mD5OfMessageAttributes;
    bool m_mD5OfMessageAttributesHasBeenSet = false;

    Aws::Map<Aws::String, MessageAttributeValue> m_messageAttributes;
    bool m_messageAttributesHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
