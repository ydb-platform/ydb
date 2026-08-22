/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/ListDeadLetterSourceQueuesRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

ListDeadLetterSourceQueuesRequest::ListDeadLetterSourceQueuesRequest() : 
    m_queueUrlHasBeenSet(false),
    m_nextTokenHasBeenSet(false),
    m_maxResults(0),
    m_maxResultsHasBeenSet(false)
{
}

Aws::String ListDeadLetterSourceQueuesRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=ListDeadLetterSourceQueues&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_nextTokenHasBeenSet)
  {
    ss << "NextToken=" << StringUtils::URLEncode(m_nextToken.c_str()) << "&";
  }

  if(m_maxResultsHasBeenSet)
  {
    ss << "MaxResults=" << m_maxResults << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  ListDeadLetterSourceQueuesRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
