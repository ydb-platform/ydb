/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/sqs/model/ResponseMetadata.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace SQS
{
namespace Model
{
  /**
   * <p>The <code>MD5OfMessageBody</code> and <code>MessageId</code>
   * elements.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/SendMessageResult">AWS
   * API Reference</a></p>
   */
  class SendMessageResult
  {
  public:
    AWS_SQS_API SendMessageResult();
    AWS_SQS_API SendMessageResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_SQS_API SendMessageResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


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
    inline void SetMD5OfMessageBody(const Aws::String& value) { m_mD5OfMessageBody = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageBody(Aws::String&& value) { m_mD5OfMessageBody = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageBody(const char* value) { m_mD5OfMessageBody.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageBody(const Aws::String& value) { SetMD5OfMessageBody(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageBody(Aws::String&& value) { SetMD5OfMessageBody(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message body string. You can use this
     * attribute to verify that Amazon SQS received the message correctly. Amazon SQS
     * URL-decodes the message before creating the MD5 digest. For information about
     * MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageBody(const char* value) { SetMD5OfMessageBody(value); return *this;}


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
    inline void SetMD5OfMessageAttributes(const Aws::String& value) { m_mD5OfMessageAttributes = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageAttributes(Aws::String&& value) { m_mD5OfMessageAttributes = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline void SetMD5OfMessageAttributes(const char* value) { m_mD5OfMessageAttributes.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageAttributes(const Aws::String& value) { SetMD5OfMessageAttributes(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageAttributes(Aws::String&& value) { SetMD5OfMessageAttributes(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message attribute string. You can use
     * this attribute to verify that Amazon SQS received the message correctly. Amazon
     * SQS URL-decodes the message before creating the MD5 digest. For information
     * about MD5, see <a href="https://www.ietf.org/rfc/rfc1321.txt">RFC1321</a>.</p>
     */
    inline SendMessageResult& WithMD5OfMessageAttributes(const char* value) { SetMD5OfMessageAttributes(value); return *this;}


    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline const Aws::String& GetMD5OfMessageSystemAttributes() const{ return m_mD5OfMessageSystemAttributes; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(const Aws::String& value) { m_mD5OfMessageSystemAttributes = value; }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(Aws::String&& value) { m_mD5OfMessageSystemAttributes = std::move(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline void SetMD5OfMessageSystemAttributes(const char* value) { m_mD5OfMessageSystemAttributes.assign(value); }

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline SendMessageResult& WithMD5OfMessageSystemAttributes(const Aws::String& value) { SetMD5OfMessageSystemAttributes(value); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline SendMessageResult& WithMD5OfMessageSystemAttributes(Aws::String&& value) { SetMD5OfMessageSystemAttributes(std::move(value)); return *this;}

    /**
     * <p>An MD5 digest of the non-URL-encoded message system attribute string. You can
     * use this attribute to verify that Amazon SQS received the message correctly.
     * Amazon SQS URL-decodes the message before creating the MD5 digest.</p>
     */
    inline SendMessageResult& WithMD5OfMessageSystemAttributes(const char* value) { SetMD5OfMessageSystemAttributes(value); return *this;}


    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline const Aws::String& GetMessageId() const{ return m_messageId; }

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline void SetMessageId(const Aws::String& value) { m_messageId = value; }

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline void SetMessageId(Aws::String&& value) { m_messageId = std::move(value); }

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline void SetMessageId(const char* value) { m_messageId.assign(value); }

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline SendMessageResult& WithMessageId(const Aws::String& value) { SetMessageId(value); return *this;}

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline SendMessageResult& WithMessageId(Aws::String&& value) { SetMessageId(std::move(value)); return *this;}

    /**
     * <p>An attribute containing the <code>MessageId</code> of the message sent to the
     * queue. For more information, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html">Queue
     * and Message Identifiers</a> in the <i>Amazon SQS Developer Guide</i>. </p>
     */
    inline SendMessageResult& WithMessageId(const char* value) { SetMessageId(value); return *this;}


    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline const Aws::String& GetSequenceNumber() const{ return m_sequenceNumber; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(const Aws::String& value) { m_sequenceNumber = value; }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(Aws::String&& value) { m_sequenceNumber = std::move(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline void SetSequenceNumber(const char* value) { m_sequenceNumber.assign(value); }

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageResult& WithSequenceNumber(const Aws::String& value) { SetSequenceNumber(value); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageResult& WithSequenceNumber(Aws::String&& value) { SetSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>This parameter applies only to FIFO (first-in-first-out) queues.</p> <p>The
     * large, non-consecutive number that Amazon SQS assigns to each message.</p>
     * <p>The length of <code>SequenceNumber</code> is 128 bits.
     * <code>SequenceNumber</code> continues to increase for a particular
     * <code>MessageGroupId</code>.</p>
     */
    inline SendMessageResult& WithSequenceNumber(const char* value) { SetSequenceNumber(value); return *this;}


    
    inline const ResponseMetadata& GetResponseMetadata() const{ return m_responseMetadata; }

    
    inline void SetResponseMetadata(const ResponseMetadata& value) { m_responseMetadata = value; }

    
    inline void SetResponseMetadata(ResponseMetadata&& value) { m_responseMetadata = std::move(value); }

    
    inline SendMessageResult& WithResponseMetadata(const ResponseMetadata& value) { SetResponseMetadata(value); return *this;}

    
    inline SendMessageResult& WithResponseMetadata(ResponseMetadata&& value) { SetResponseMetadata(std::move(value)); return *this;}

  private:

    Aws::String m_mD5OfMessageBody;

    Aws::String m_mD5OfMessageAttributes;

    Aws::String m_mD5OfMessageSystemAttributes;

    Aws::String m_messageId;

    Aws::String m_sequenceNumber;

    ResponseMetadata m_responseMetadata;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
