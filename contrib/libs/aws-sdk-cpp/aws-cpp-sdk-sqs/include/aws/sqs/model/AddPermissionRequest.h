/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/sqs/SQSRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <utility>

namespace Aws
{
namespace SQS
{
namespace Model
{

  /**
   * <p/><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/AddPermissionRequest">AWS
   * API Reference</a></p>
   */
  class AddPermissionRequest : public SQSRequest
  {
  public:
    AWS_SQS_API AddPermissionRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "AddPermission"; }

    AWS_SQS_API Aws::String SerializePayload() const override;

  protected:
    AWS_SQS_API void DumpBodyToUrl(Aws::Http::URI& uri ) const override;

  public:

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline const Aws::String& GetQueueUrl() const{ return m_queueUrl; }

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline bool QueueUrlHasBeenSet() const { return m_queueUrlHasBeenSet; }

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const Aws::String& value) { m_queueUrlHasBeenSet = true; m_queueUrl = value; }

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(Aws::String&& value) { m_queueUrlHasBeenSet = true; m_queueUrl = std::move(value); }

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline void SetQueueUrl(const char* value) { m_queueUrlHasBeenSet = true; m_queueUrl.assign(value); }

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline AddPermissionRequest& WithQueueUrl(const Aws::String& value) { SetQueueUrl(value); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline AddPermissionRequest& WithQueueUrl(Aws::String&& value) { SetQueueUrl(std::move(value)); return *this;}

    /**
     * <p>The URL of the Amazon SQS queue to which permissions are added.</p> <p>Queue
     * URLs and names are case-sensitive.</p>
     */
    inline AddPermissionRequest& WithQueueUrl(const char* value) { SetQueueUrl(value); return *this;}


    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline const Aws::String& GetLabel() const{ return m_label; }

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline bool LabelHasBeenSet() const { return m_labelHasBeenSet; }

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline void SetLabel(const Aws::String& value) { m_labelHasBeenSet = true; m_label = value; }

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline void SetLabel(Aws::String&& value) { m_labelHasBeenSet = true; m_label = std::move(value); }

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline void SetLabel(const char* value) { m_labelHasBeenSet = true; m_label.assign(value); }

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline AddPermissionRequest& WithLabel(const Aws::String& value) { SetLabel(value); return *this;}

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline AddPermissionRequest& WithLabel(Aws::String&& value) { SetLabel(std::move(value)); return *this;}

    /**
     * <p>The unique identification of the permission you're setting (for example,
     * <code>AliceSendMessage</code>). Maximum 80 characters. Allowed characters
     * include alphanumeric characters, hyphens (<code>-</code>), and underscores
     * (<code>_</code>).</p>
     */
    inline AddPermissionRequest& WithLabel(const char* value) { SetLabel(value); return *this;}


    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAWSAccountIds() const{ return m_aWSAccountIds; }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline bool AWSAccountIdsHasBeenSet() const { return m_aWSAccountIdsHasBeenSet; }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline void SetAWSAccountIds(const Aws::Vector<Aws::String>& value) { m_aWSAccountIdsHasBeenSet = true; m_aWSAccountIds = value; }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline void SetAWSAccountIds(Aws::Vector<Aws::String>&& value) { m_aWSAccountIdsHasBeenSet = true; m_aWSAccountIds = std::move(value); }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline AddPermissionRequest& WithAWSAccountIds(const Aws::Vector<Aws::String>& value) { SetAWSAccountIds(value); return *this;}

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline AddPermissionRequest& WithAWSAccountIds(Aws::Vector<Aws::String>&& value) { SetAWSAccountIds(std::move(value)); return *this;}

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline AddPermissionRequest& AddAWSAccountIds(const Aws::String& value) { m_aWSAccountIdsHasBeenSet = true; m_aWSAccountIds.push_back(value); return *this; }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline AddPermissionRequest& AddAWSAccountIds(Aws::String&& value) { m_aWSAccountIdsHasBeenSet = true; m_aWSAccountIds.push_back(std::move(value)); return *this; }

    /**
     * <p>The Amazon Web Services account numbers of the <a
     * href="https://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">principals</a>
     * who are to receive permission. For information about locating the Amazon Web
     * Services account identification, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests.html#sqs-api-request-authentication">Your
     * Amazon Web Services Identifiers</a> in the <i>Amazon SQS Developer
     * Guide</i>.</p>
     */
    inline AddPermissionRequest& AddAWSAccountIds(const char* value) { m_aWSAccountIdsHasBeenSet = true; m_aWSAccountIds.push_back(value); return *this; }


    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline const Aws::Vector<Aws::String>& GetActions() const{ return m_actions; }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline bool ActionsHasBeenSet() const { return m_actionsHasBeenSet; }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline void SetActions(const Aws::Vector<Aws::String>& value) { m_actionsHasBeenSet = true; m_actions = value; }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline void SetActions(Aws::Vector<Aws::String>&& value) { m_actionsHasBeenSet = true; m_actions = std::move(value); }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline AddPermissionRequest& WithActions(const Aws::Vector<Aws::String>& value) { SetActions(value); return *this;}

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline AddPermissionRequest& WithActions(Aws::Vector<Aws::String>&& value) { SetActions(std::move(value)); return *this;}

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline AddPermissionRequest& AddActions(const Aws::String& value) { m_actionsHasBeenSet = true; m_actions.push_back(value); return *this; }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline AddPermissionRequest& AddActions(Aws::String&& value) { m_actionsHasBeenSet = true; m_actions.push_back(std::move(value)); return *this; }

    /**
     * <p>The action the client wants to allow for the specified principal. Valid
     * values: the name of any action or <code>*</code>.</p> <p>For more information
     * about these actions, see <a
     * href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-overview-of-managing-access.html">Overview
     * of Managing Access Permissions to Your Amazon Simple Queue Service Resource</a>
     * in the <i>Amazon SQS Developer Guide</i>.</p> <p>Specifying
     * <code>SendMessage</code>, <code>DeleteMessage</code>, or
     * <code>ChangeMessageVisibility</code> for <code>ActionName.n</code> also grants
     * permissions for the corresponding batch versions of those actions:
     * <code>SendMessageBatch</code>, <code>DeleteMessageBatch</code>, and
     * <code>ChangeMessageVisibilityBatch</code>.</p>
     */
    inline AddPermissionRequest& AddActions(const char* value) { m_actionsHasBeenSet = true; m_actions.push_back(value); return *this; }

  private:

    Aws::String m_queueUrl;
    bool m_queueUrlHasBeenSet = false;

    Aws::String m_label;
    bool m_labelHasBeenSet = false;

    Aws::Vector<Aws::String> m_aWSAccountIds;
    bool m_aWSAccountIdsHasBeenSet = false;

    Aws::Vector<Aws::String> m_actions;
    bool m_actionsHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws
