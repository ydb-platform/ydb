/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/UntagQueueRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

UntagQueueRequest::UntagQueueRequest() : 
    m_queueUrlHasBeenSet(false),
    m_tagKeysHasBeenSet(false)
{
}

Aws::String UntagQueueRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=UntagQueue&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_tagKeysHasBeenSet)
  {
    unsigned tagKeysCount = 1;
    for(auto& item : m_tagKeys)
    {
      ss << "TagKey." << tagKeysCount << "="
          << StringUtils::URLEncode(item.c_str()) << "&";
      tagKeysCount++;
    }
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  UntagQueueRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
