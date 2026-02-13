/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

CreateQueueRequest::CreateQueueRequest() : 
    m_queueNameHasBeenSet(false),
    m_attributesHasBeenSet(false),
    m_tagsHasBeenSet(false)
{
}

Aws::String CreateQueueRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=CreateQueue&";
  if(m_queueNameHasBeenSet)
  {
    ss << "QueueName=" << StringUtils::URLEncode(m_queueName.c_str()) << "&";
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

  if(m_tagsHasBeenSet)
  {
    unsigned tagsCount = 1;
    for(auto& item : m_tags)
    {
      ss << "Tag." << tagsCount << ".Key="
          << StringUtils::URLEncode(item.first.c_str()) << "&";
      ss << "Tag." << tagsCount << ".Value="
          << StringUtils::URLEncode(item.second.c_str()) << "&";
      tagsCount++;
    }
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  CreateQueueRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
