/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

ReceiveMessageRequest::ReceiveMessageRequest() : 
    m_queueUrlHasBeenSet(false),
    m_attributeNamesHasBeenSet(false),
    m_messageAttributeNamesHasBeenSet(false),
    m_maxNumberOfMessages(0),
    m_maxNumberOfMessagesHasBeenSet(false),
    m_visibilityTimeout(0),
    m_visibilityTimeoutHasBeenSet(false),
    m_waitTimeSeconds(0),
    m_waitTimeSecondsHasBeenSet(false),
    m_receiveRequestAttemptIdHasBeenSet(false)
{
}

Aws::String ReceiveMessageRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=ReceiveMessage&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_attributeNamesHasBeenSet)
  {
    unsigned attributeNamesCount = 1;
    for(auto& item : m_attributeNames)
    {
      ss << "AttributeName." << attributeNamesCount << "="
          << StringUtils::URLEncode(QueueAttributeNameMapper::GetNameForQueueAttributeName(item).c_str()) << "&";
      attributeNamesCount++;
    }
  }

  if(m_messageAttributeNamesHasBeenSet)
  {
    unsigned messageAttributeNamesCount = 1;
    for(auto& item : m_messageAttributeNames)
    {
      ss << "MessageAttributeName." << messageAttributeNamesCount << "="
          << StringUtils::URLEncode(item.c_str()) << "&";
      messageAttributeNamesCount++;
    }
  }

  if(m_maxNumberOfMessagesHasBeenSet)
  {
    ss << "MaxNumberOfMessages=" << m_maxNumberOfMessages << "&";
  }

  if(m_visibilityTimeoutHasBeenSet)
  {
    ss << "VisibilityTimeout=" << m_visibilityTimeout << "&";
  }

  if(m_waitTimeSecondsHasBeenSet)
  {
    ss << "WaitTimeSeconds=" << m_waitTimeSeconds << "&";
  }

  if(m_receiveRequestAttemptIdHasBeenSet)
  {
    ss << "ReceiveRequestAttemptId=" << StringUtils::URLEncode(m_receiveRequestAttemptId.c_str()) << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  ReceiveMessageRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
