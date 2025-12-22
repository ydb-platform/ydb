/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/TagQueueRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

TagQueueRequest::TagQueueRequest() : 
    m_queueUrlHasBeenSet(false),
    m_tagsHasBeenSet(false)
{
}

Aws::String TagQueueRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=TagQueue&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
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


void  TagQueueRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
