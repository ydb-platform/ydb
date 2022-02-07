/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/model/ListBucketsResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/StringUtils.h>

#include <utility>

using namespace Aws::S3::Model;
using namespace Aws::Utils::Xml;
using namespace Aws::Utils;
using namespace Aws;

ListBucketsResult::ListBucketsResult()
{
}

ListBucketsResult::ListBucketsResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

ListBucketsResult& ListBucketsResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode resultNode = xmlDocument.GetRootElement();

  if(!resultNode.IsNull())
  {
    XmlNode bucketsNode = resultNode.FirstChild("Buckets");
    if(!bucketsNode.IsNull())
    {
      XmlNode bucketsMember = bucketsNode.FirstChild("Bucket");
      while(!bucketsMember.IsNull())
      {
        m_buckets.push_back(bucketsMember);
        bucketsMember = bucketsMember.NextNode("Bucket");
      }

    }
    XmlNode ownerNode = resultNode.FirstChild("Owner");
    if(!ownerNode.IsNull())
    {
      m_owner = ownerNode;
    }
  }

  return *this;
}
