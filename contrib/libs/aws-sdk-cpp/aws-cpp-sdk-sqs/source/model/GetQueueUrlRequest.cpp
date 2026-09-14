/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

GetQueueUrlRequest::GetQueueUrlRequest() : 
    m_queueNameHasBeenSet(false),
    m_queueOwnerAWSAccountIdHasBeenSet(false)
{
}

Aws::String GetQueueUrlRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=GetQueueUrl&";
  if(m_queueNameHasBeenSet)
  {
    ss << "QueueName=" << StringUtils::URLEncode(m_queueName.c_str()) << "&";
  }

  if(m_queueOwnerAWSAccountIdHasBeenSet)
  {
    ss << "QueueOwnerAWSAccountId=" << StringUtils::URLEncode(m_queueOwnerAWSAccountId.c_str()) << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  GetQueueUrlRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
