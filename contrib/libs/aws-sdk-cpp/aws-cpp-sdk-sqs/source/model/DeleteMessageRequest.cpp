/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

DeleteMessageRequest::DeleteMessageRequest() : 
    m_queueUrlHasBeenSet(false),
    m_receiptHandleHasBeenSet(false)
{
}

Aws::String DeleteMessageRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=DeleteMessage&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_receiptHandleHasBeenSet)
  {
    ss << "ReceiptHandle=" << StringUtils::URLEncode(m_receiptHandle.c_str()) << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  DeleteMessageRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
