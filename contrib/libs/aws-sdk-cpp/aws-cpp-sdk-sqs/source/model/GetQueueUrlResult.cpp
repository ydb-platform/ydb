/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/GetQueueUrlResult.h>
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

GetQueueUrlResult::GetQueueUrlResult()
{
}

GetQueueUrlResult::GetQueueUrlResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

GetQueueUrlResult& GetQueueUrlResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode rootNode = xmlDocument.GetRootElement();
  XmlNode resultNode = rootNode;
  if (!rootNode.IsNull() && (rootNode.GetName() != "GetQueueUrlResult"))
  {
    resultNode = rootNode.FirstChild("GetQueueUrlResult");
  }

  if(!resultNode.IsNull())
  {
    XmlNode queueUrlNode = resultNode.FirstChild("QueueUrl");
    if(!queueUrlNode.IsNull())
    {
      m_queueUrl = Aws::Utils::Xml::DecodeEscapedXmlText(queueUrlNode.GetText());
    }
  }

  if (!rootNode.IsNull()) {
    XmlNode responseMetadataNode = rootNode.FirstChild("ResponseMetadata");
    m_responseMetadata = responseMetadataNode;
    AWS_LOGSTREAM_DEBUG("Aws::SQS::Model::GetQueueUrlResult", "x-amzn-request-id: " << m_responseMetadata.GetRequestId() );
  }
  return *this;
}
