/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SetQueueAttributesRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

SetQueueAttributesRequest::SetQueueAttributesRequest() : 
    m_queueUrlHasBeenSet(false),
    m_attributesHasBeenSet(false)
{
}

Aws::String SetQueueAttributesRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=SetQueueAttributes&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_attributesHasBeenSet)
  {
    unsigned attributesCount = 1;
    for(auto& item : m_attributes)
    {
      ss << "Attribute." << attributesCount << ".Name="
          << StringUtils::URLEncode(QueueAttributeNameMapper::GetNameForQueueAttributeName(item.first).c_str()) << "&";
      ss << "Attribute." << attributesCount << ".Value="
          << StringUtils::URLEncode(item.second.c_str()) << "&";
      attributesCount++;
    }
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  SetQueueAttributesRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
