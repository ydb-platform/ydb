/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/sqs/SQSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace SQS
{
namespace Model
{

  /**
   */
  class ListQueueTagsRequest : public SQSRequest
  {
  public:
    AWS_SQS_API ListQueueTagsRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListQueueTags"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the queue.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the queue.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the queue.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the queue.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the queue.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the queue.</p>
     */
    inline ListQueueTagsRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the queue.</p>
     */
    inline ListQueueTagsRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the queue.</p>
     */
    inline ListQueueTagsRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
