/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

GetQueueAttributesRequest::GetQueueAttributesRequest() : 
    m_queueUrlHasBeenSet(false),
    m_attributeNamesHasBeenSet(false)
{
}

Aws::String GetQueueAttributesRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=GetQueueAttributes&";
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

  ss << "Version=2012-11-05";
  return ss.str();
}


void  GetQueueAttributesRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
