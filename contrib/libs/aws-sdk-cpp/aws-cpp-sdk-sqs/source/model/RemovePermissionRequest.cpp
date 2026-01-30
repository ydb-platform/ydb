/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/RemovePermissionRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

RemovePermissionRequest::RemovePermissionRequest() : 
    m_queueUrlHasBeenSet(false),
    m_labelHasBeenSet(false)
{
}

Aws::String RemovePermissionRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=RemovePermission&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_labelHasBeenSet)
  {
    ss << "Label=" << StringUtils::URLEncode(m_label.c_str()) << "&";
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  RemovePermissionRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
