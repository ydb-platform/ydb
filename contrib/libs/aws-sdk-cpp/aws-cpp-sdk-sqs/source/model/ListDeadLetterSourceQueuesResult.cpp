/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/ListDeadLetterSourceQueuesResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>

#include <utility>

using namespace Aws::SQS::Model;
using namespace Aws::Utils::Xml;
using namespace Aws::Utils::Logging;
using namespace Aws::Utils;
using namespace Aws;

ListDeadLetterSourceQueuesResult::ListDeadLetterSourceQueuesResult()
{
}

ListDeadLetterSourceQueuesResult::ListDeadLetterSourceQueuesResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

ListDeadLetterSourceQueuesResult& ListDeadLetterSourceQueuesResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode rootNode = xmlDocument.GetRootElement();
  XmlNode resultNode = rootNode;
  if (!rootNode.IsNull() && (rootNode.GetName() != "ListDeadLetterSourceQueuesResult"))
  {
    resultNode = rootNode.FirstChild("ListDeadLetterSourceQueuesResult");
  }

  if(!resultNode.IsNull())
  {
    XmlNode queueUrlsNode = resultNode.FirstChild("QueueUrl");
    if(!queueUrlsNode.IsNull())
    {
      XmlNode queueUrlMember = queueUrlsNode;
      while(!queueUrlMember.IsNull())
      {
        m_queueUrls.push_back(queueUrlMember.GetText());
        queueUrlMember = queueUrlMember.NextNode("QueueUrl");
      }

    }
    XmlNode nextTokenNode = resultNode.FirstChild("NextToken");
    if(!nextTokenNode.IsNull())
    {
      m_nextToken = Aws::Utils::Xml::DecodeEscapedXmlText(nextTokenNode.GetText());
    }
  }

  if (!rootNode.IsNull()) {
    XmlNode responseMetadataNode = rootNode.FirstChild("ResponseMetadata");
    m_responseMetadata = responseMetadataNode;
    AWS_LOGSTREAM_DEBUG("Aws::SQS::Model::ListDeadLetterSourceQueuesResult", "x-amzn-request-id: " << m_responseMetadata.GetRequestId() );
  }
  return *this;
}
