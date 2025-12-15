/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/sqs/SQSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/sqs/model/DeleteMessageBatchRequestEntry.h>
#include <utility>

namespace Aws
{
namespace SQS
{
namespace Model
{

  /**
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteMessageBatchRequest">AWS
   * API Reference</a></p>
   */
  class DeleteMessageBatchRequest : public SQSRequest
  {
  public:
    AWS_SQS_API DeleteMessageBatchRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "DeleteMessageBatch"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline DeleteMessageBatchRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline DeleteMessageBatchRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which messages are deleted.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline DeleteMessageBatchRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline const Aws::Vector<DeleteMessageBatchRequestEntry>& GetEntries() const{ return m_entries; }

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline bool EntriesHasBeenSet() const { return m_entriesHasBeenSet; }

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline void SetEntries(const Aws::Vector<DeleteMessageBatchRequestEntry>& value) { m_entriesHasBeenSet = true; m_entries = value; }

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline void SetEntries(Aws::Vector<DeleteMessageBatchRequestEntry>&& value) { m_entriesHasBeenSet = true; m_entries = std::move(value); }

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline DeleteMessageBatchRequest& WithEntries(const Aws::Vector<DeleteMessageBatchRequestEntry>& value) { SetEntries(value); return *this;}

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline DeleteMessageBatchRequest& WithEntries(Aws::Vector<DeleteMessageBatchRequestEntry>&& value) { SetEntries(std::move(value)); return *this;}

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline DeleteMessageBatchRequest& AddEntries(const DeleteMessageBatchRequestEntry& value) { m_entriesHasBeenSet = true; m_entries.push_back(value); return *this; }

    /**
     * <p>A list of receipt handles for the messages to be deleted.</p>
     */
    inline DeleteMessageBatchRequest& AddEntries(DeleteMessageBatchRequestEntry&& value) { m_entriesHasBeenSet = true; m_entries.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;

    Aws::Vector<DeleteMessageBatchRequestEntry> m_entries;
    bool m_entriesHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
