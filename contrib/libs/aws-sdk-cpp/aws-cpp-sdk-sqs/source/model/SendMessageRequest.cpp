/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

SendMessageRequest::SendMessageRequest() : 
    m_queueUrlHasBeenSet(false),
    m_messageBodyHasBeenSet(false),
    m_delaySeconds(0),
    m_delaySecondsHasBeenSet(false),
    m_messageAttributesHasBeenSet(false),
    m_messageSystemAttributesHasBeenSet(false),
    m_messageDeduplicationIdHasBeenSet(false),
    m_messageGroupIdHasBeenSet(false)
{
}

Aws::String SendMessageRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=SendMessage&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_messageBodyHasBeenSet)
  {
    ss << "MessageBody=" << StringUtils::URLEncode(m_messageBody.c_str()) << "&";
  }

  if(m_delaySecondsHasBeenSet)
  {
    ss << "DelaySeconds=" << m_delaySeconds << "&";
  }

  if(m_messageAttributesHasBeenSet)
  {
    unsigned messageAttributesCount = 1;
    for(auto& item : m_messageAttributes)
    {
      ss << "MessageAttribute." << messageAttributesCount << ".Name="
          << StringUtils::URLEncode(item.first.c_str()) << "&";
      item.second.OutputToStream(ss, "MessageAttribute.", messageAttributesCount, ".Value");
      messageAttributesCount++;
    }
  }

  if(m_messageSystemAttributesHasBeenSet)
  {
    unsigned messageSystemAttributesCount = 1;
    for(auto& item : m_messageSystemAttributes)
    {
      ss << "MessageSystemAttribute." << messageSystemAttributesCount << ".Name="
          << StringUtils::URLEncode(MessageSystemAttributeNameForSendsMapper::GetNameForMessageSystemAttributeNameForSends(item.first).c_str()) << "&";
      item.second.OutputToStream(ss, "MessageSystemAttribute.", messageSystemAttributesCount, ".Value");
      messageSystemAttributesCount++;
    }
  }

  if(m_messageDeduplicationIdHasBeenSet)
  {
    ss << "MessageDeduplicationId=" << StringUtils::URLEncode(m_messageDeduplicationId.c_str()) << "&";
  }

  if(m_messageGroupIdHasBeenSet)
  {
    ss << "MessageGroupId=" << StringUtils::URLEncode(m_messageGroupId.c_str()) << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  SendMessageRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
