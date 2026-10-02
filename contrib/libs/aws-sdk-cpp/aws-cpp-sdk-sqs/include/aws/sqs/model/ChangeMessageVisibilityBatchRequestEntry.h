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
   * <p>Encloses a receipt handle and an entry id for each message in <code>
   * <a>ChangeMessageVisibilityBatch</a>.</code> </p>  <p>All of the
   * following list parameters must be prefixed with
   * <code>ChangeMessageVisibilityBatchRequestEntry.n</code>, where <code>n</code> is
   * an integer value starting with <code>1</code>. For example, a parameter list for
   * this action might look like this:</p>  <p>
   * <code>&amp;ChangeMessageVisibilityBatchRequestEntry.1.Id=change_visibility_msg_2</code>
   * </p> <p>
   * <code>&amp;ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=your_receipt_handle</code>
   * </p> <p>
   * <code>&amp;ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=45</code>
   * </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/ChangeMessageVisibilityBatchRequestEntry">AWS
   * API Reference</a></p>
   */
  class ChangeMessageVisibilityBatchRequestEntry
  {
  public:
    AWS_SQS_API ChangeMessageVisibilityBatchRequestEntry();
    AWS_SQS_API ChangeMessageVisibilityBatchRequestEntry(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_SQS_API ChangeMessageVisibilityBatchRequestEntry& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_SQS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_SQS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline bool IdHasBeenSet() const { return m_idHasBeenSet; }

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * <p>An identifier for this particular receipt handle used to communicate the
     * result.</p>  <p>The <code>Id</code>s of a batch request need to be unique
     * within a request.</p> <p>This identifier can have up to 80 characters. The
     * following characters are accepted: alphanumeric characters, hyphens(-), and
     * underscores (_).</p> 
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithId(const char* value) { SetId(value); return *this;}


    /**
     * <p>A receipt handle.</p>
     */
    inline const Aws::String& GetReceiptHandle() const{ return m_receiptHandle; }

    /**
     * <p>A receipt handle.</p>
     */
    inline bool ReceiptHandleHasBeenSet() const { return m_receiptHandleHasBeenSet; }

    /**
     * <p>A receipt handle.</p>
     */
    inline void SetReceiptHandle(const Aws::String& value) { m_receiptHandleHasBeenSet = true; m_receiptHandle = value; }

    /**
     * <p>A receipt handle.</p>
     */
    inline void SetReceiptHandle(Aws::String&& value) { m_receiptHandleHasBeenSet = true; m_receiptHandle = std::move(value); }

    /**
     * <p>A receipt handle.</p>
     */
    inline void SetReceiptHandle(const char* value) { m_receiptHandleHasBeenSet = true; m_receiptHandle.assign(value); }

    /**
     * <p>A receipt handle.</p>
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithReceiptHandle(const Aws::String& value) { SetReceiptHandle(value); return *this;}

    /**
     * <p>A receipt handle.</p>
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithReceiptHandle(Aws::String&& value) { SetReceiptHandle(std::move(value)); return *this;}

    /**
     * <p>A receipt handle.</p>
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithReceiptHandle(const char* value) { SetReceiptHandle(value); return *this;}


    /**
     * <p>The new value (in seconds) for the message's visibility timeout.</p>
     */
    inline int GetVisibilityTimeout() const{ return m_visibilityTimeout; }

    /**
     * <p>The new value (in seconds) for the message's visibility timeout.</p>
     */
    inline bool VisibilityTimeoutHasBeenSet() const { return m_visibilityTimeoutHasBeenSet; }

    /**
     * <p>The new value (in seconds) for the message's visibility timeout.</p>
     */
    inline void SetVisibilityTimeout(int value) { m_visibilityTimeoutHasBeenSet = true; m_visibilityTimeout = value; }

    /**
     * <p>The new value (in seconds) for the message's visibility timeout.</p>
     */
    inline ChangeMessageVisibilityBatchRequestEntry& WithVisibilityTimeout(int value) { SetVisibilityTimeout(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet = false;

    Aws::String m_receiptHandle;
    bool m_receiptHandleHasBeenSet = false;

    int m_visibilityTimeout;
    bool m_visibilityTimeoutHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
