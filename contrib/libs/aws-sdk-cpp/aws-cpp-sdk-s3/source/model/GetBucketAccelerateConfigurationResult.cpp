/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/model/GetBucketAccelerateConfigurationResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/StringUtils.h>

#include <utility>

using namespace Aws::S3::Model;
using namespace Aws::Utils::Xml;
using namespace Aws::Utils;
using namespace Aws;

GetBucketAccelerateConfigurationResult::GetBucketAccelerateConfigurationResult() : 
    m_status(BucketAccelerateStatus::NOT_SET)
{
}

GetBucketAccelerateConfigurationResult::GetBucketAccelerateConfigurationResult(const Aws::AmazonWebServiceResult<XmlDocument>& result) : 
    m_status(BucketAccelerateStatus::NOT_SET)
{
  *this = result;
}

GetBucketAccelerateConfigurationResult& GetBucketAccelerateConfigurationResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode resultNode = xmlDocument.GetRootElement();

  if(!resultNode.IsNull())
  {
    XmlNode statusNode = resultNode.FirstChild("Status");
    if(!statusNode.IsNull())
    {
      m_status = BucketAccelerateStatusMapper::GetBucketAccelerateStatusForName(StringUtils::Trim(Aws::Utils::Xml::DecodeEscapedXmlText(statusNode.GetText()).c_str()).c_str());
    }
  }

  return *this;
}
