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
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/RemovePermissionRequest">AWS
   * API Reference</a></p>
   */
  class RemovePermissionRequest : public SQSRequest
  {
  public:
    AWS_SQS_API RemovePermissionRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "RemovePermission"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline RemovePermissionRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline RemovePermissionRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue from which permissions are removed.</p>
     * <p>Queue URLs and names are case-sensitive.</p>
     */
    inline RemovePermissionRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline const Aws::String& GetLabel() const{ return m_label; }

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline bool LabelHasBeenSet() const { return m_labelHasBeenSet; }

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline void SetLabel(const Aws::String& value) { m_labelHasBeenSet = true; m_label = value; }

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline void SetLabel(Aws::String&& value) { m_labelHasBeenSet = true; m_label = std::move(value); }

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline void SetLabel(const char* value) { m_labelHasBeenSet = true; m_label.assign(value); }

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline RemovePermissionRequest& WithLabel(const Aws::String& value) { SetLabel(value); return *this;}

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline RemovePermissionRequest& WithLabel(Aws::String&& value) { SetLabel(std::move(value)); return *this;}

    /**
     * <p>The identification of the permission to remove. This is the label added using
     * the <code> <a>AddPermission</a> </code> action.</p>
     */
    inline RemovePermissionRequest& WithLabel(const char* value) { SetLabel(value); return *this;}

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;

    Aws::String m_label;
    bool m_labelHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
