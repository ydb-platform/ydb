/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/AddPermissionRequest.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

using namespace Aws::SQS::Model;
using namespace Aws::Utils;

AddPermissionRequest::AddPermissionRequest() : 
    m_queueUrlHasBeenSet(false),
    m_labelHasBeenSet(false),
    m_aWSAccountIdsHasBeenSet(false),
    m_actionsHasBeenSet(false)
{
}

Aws::String AddPermissionRequest::SerializePayload() const
{
  Aws::StringStream ss;
  ss << "Action=AddPermission&";
  if(m_queueUrlHasBeenSet)
  {
    ss << "QueueUrl=" << StringUtils::URLEncode(m_queueUrl.c_str()) << "&";
  }

  if(m_labelHasBeenSet)
  {
    ss << "Label=" << StringUtils::URLEncode(m_label.c_str()) << "&";
  }

  if(m_aWSAccountIdsHasBeenSet)
  {
    unsigned aWSAccountIdsCount = 1;
    for(auto& item : m_aWSAccountIds)
    {
      ss << "AWSAccountId." << aWSAccountIdsCount << "="
          << StringUtils::URLEncode(item.c_str()) << "&";
      aWSAccountIdsCount++;
    }
  }

  if(m_actionsHasBeenSet)
  {
    unsigned actionsCount = 1;
    for(auto& item : m_actions)
    {
      ss << "ActionName." << actionsCount << "="
          << StringUtils::URLEncode(item.c_str()) << "&";
      actionsCount++;
    }
  }

  ss << "Version=2012-11-05";
  return ss.str();
}


void  AddPermissionRequest::DumpBodyToUrl(Aws::Http::URI& uri ) const
{
  uri.SetQueryString(SerializePayload());
}
