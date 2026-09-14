/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
   * <p>Encloses a <code>MessageId</code> for a successfully-enqueued message in a
   * <code> <a>SendMessageBatch</a>.</code> </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/SendMessageBatchResultEntry">AWS
   * API Reference</a></p>
   */
  class SendMessageBatchResultEntry
  {
  public:
    AWS_SQS_API SendMessageBatchResultEntry();
    AWS_SQS_API SendMessageBatchResultEntry(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_SQS_API SendMessageBatchResultEntry& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_SQS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_SQS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline bool IdHasBeenSet() const { return m_idHasBeenSet; }

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline SendMessageBatchResultEntry& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline SendMessageBatchResultEntry& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * <p>An identifier for the message in this batch.</p>
     */
    inline SendMessageBatchResultEntry& WithId(const char* value) { SetId(value); return *this;}


    /**
     * <p>An identifier for the message.</p>
     */
    inline const Aws::String& GetMessageId() const{ return m_messageId; }

    /**
     * <p>An identifier for the message.</p>
     */
    inline bool MessageIdHasBeenSet() const { return m_messageIdHasBeenSet; }

    /**
     * <p>An identifier for the message.</p>
     */
    inline void SetMessageId(const Aws::String& value) { m_messageIdHasBeenSet = true; m_messageId = value; }

    /**
     * <p>An identifier for the message.</p>
     */
    inline void SetMessageId(Aws::String&& value) { m_messageIdHasBeenSet = true; m_messageId = std::move(value); }

    /**
     * <p>An identifier for the message.</p>
     */
    inline void SetMessageId(const char* value) { m_messageIdHasBeenSet = true; m_messageId.assign(value); }

    /**
     * <p>An identifier for the message.</p>
     */
    inline SendMessageBatchResultEntry& WithMessageId(const Aws::String& value) { SetMessageId(value); return *this;}

    /**
     * <p>An identifier for the message.</p>
     */
    inline SendMessageBatchResultEntry& WithMessageId(Aws::String&& value) { SetMessageId(std::move(value)); return *this;}

    /**
     * <p>An identifier for the message.</p>
     */
    inline SendMessageBatchResultEntry& WithMessageId(const char* value) { SetMessageId(value); return *this;}


    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline const Aws::String& GetMD5OfMessageBody() const{ return m_mD5OfMessageBody; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline bool MD5OfMessageBodyHasBeenSet() const { return m_mD5OfMessageBodyHasBeenSet; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageBody(const Aws::String& value) { m_mD5OfMessageBodyHasBeenSet = true; m_mD5OfMessageBody = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageBody(Aws::String&& value) { m_mD5OfMessageBodyHasBeenSet = true; m_mD5OfMessageBody = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageBody(const char* value) { m_mD5OfMessageBodyHasBeenSet = true; m_mD5OfMessageBody.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageBody(const Aws::String& value) { SetMD5OfMessageBody(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageBody(Aws::String&& value) { SetMD5OfMessageBody(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageBody(const char* value) { SetMD5OfMessageBody(value); return *this;}


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
    inline SendMessageBatchResultEntry& WithMD5OfMessageAttributes(const Aws::String& value) { SetMD5OfMessageAttributes(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageAttributes(Aws::String&& value) { SetMD5OfMessageAttributes(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageAttributes(const char* value) { SetMD5OfMessageAttributes(value); return *this;}


    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline const Aws::String& GetMD5OfMessageSystemAttributes() const{ return m_mD5OfMessageSystemAttributes; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline bool MD5OfMessageSystemAttributesHasBeenSet() const { return m_mD5OfMessageSystemAttributesHasBeenSet; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(const Aws::String& value) { m_mD5OfMessageSystemAttributesHasBeenSet = true; m_mD5OfMessageSystemAttributes = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(Aws::String&& value) { m_mD5OfMessageSystemAttributesHasBeenSet = true; m_mD5OfMessageSystemAttributes = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(const char* value) { m_mD5OfMessageSystemAttributesHasBeenSet = true; m_mD5OfMessageSystemAttributes.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageSystemAttributes(const Aws::String& value) { SetMD5OfMessageSystemAttributes(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageSystemAttributes(Aws::String&& value) { SetMD5OfMessageSystemAttributes(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest. For
     * information about MD5, see <a
     * href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageBatchResultEntry& WithMD5OfMessageSystemAttributes(const char* value) { SetMD5OfMessageSystemAttributes(value); return *this;}


    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline const Aws::String& GetSequenceNumber() const{ return m_sequenceNumber; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline bool SequenceNumberHasBeenSet() const { return m_sequenceNumberHasBeenSet; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(const Aws::String& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = value; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(Aws::String&& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = std::move(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(const char* value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber.assign(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageBatchResultEntry& WithSequenceNumber(const Aws::String& value) { SetSequenceNumber(value); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageBatchResultEntry& WithSequenceNumber(Aws::String&& value) { SetSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits. As
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageBatchResultEntry& WithSequenceNumber(const char* value) { SetSequenceNumber(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet = false;

    Aws::String m_messageId;
    bool m_messageIdHasBeenSet = false;

    Aws::String m_mD5OfMessageBody;
    bool m_mD5OfMessageBodyHasBeenSet = false;

    Aws::String m_mD5OfMessageAttributes;
    bool m_mD5OfMessageAttributesHasBeenSet = false;

    Aws::String m_mD5OfMessageSystemAttributes;
    bool m_mD5OfMessageSystemAttributesHasBeenSet = false;

    Aws::String m_sequenceNumber;
    bool m_sequenceNumberHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
