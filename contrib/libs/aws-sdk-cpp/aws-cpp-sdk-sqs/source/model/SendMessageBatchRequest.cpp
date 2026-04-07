/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

SendMessageBatchRequest::SendMessageBatchRequest() : 
    m_queueUrlHasBeenSet(false),
    m_entriesHasBeenSet(false)
{
}

Aws::String SendMessageBatchRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=SendMessageBatch&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_entriesHasBeenSet)
  {
    unsigned entriesCount = 1;
    for(auto& item : m_entries)
    {
      item.OutputToStream(ss, "SendMessageBatchRequestEntry.", entriesCount, "");
      entriesCount++;
    }
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  SendMessageBatchRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
