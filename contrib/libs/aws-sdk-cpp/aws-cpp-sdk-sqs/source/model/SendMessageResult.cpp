/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageResult.h>
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

SendMessageResult::SendMessageResult()
{
}

SendMessageResult::SendMessageResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

SendMessageResult& SendMessageResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode rootNode = xmlDocument.GetRootElement();
  XmlNode resultNode = rootNode;
  if (!rootNode.IsNull() && (rootNode.GetName() != "SendMessageResult"))
  {
    resultNode = rootNode.FirstChild("SendMessageResult");
  }

  if(!resultNode.IsNull())
  {
    XmlNode mD5OfMessageBodyNode = resultNode.FirstChild("MD5OfMessageBody");
    if(!mD5OfMessageBodyNode.IsNull())
    {
      m_mD5OfMessageBody = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageBodyNode.GetText());
    }
    XmlNode mD5OfMessageAttributesNode = resultNode.FirstChild("MD5OfMessageAttributes");
    if(!mD5OfMessageAttributesNode.IsNull())
    {
      m_mD5OfMessageAttributes = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageAttributesNode.GetText());
    }
    XmlNode mD5OfMessageSystemAttributesNode = resultNode.FirstChild("MD5OfMessageSystemAttributes");
    if(!mD5OfMessageSystemAttributesNode.IsNull())
    {
      m_mD5OfMessageSystemAttributes = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageSystemAttributesNode.GetText());
    }
    XmlNode messageIdNode = resultNode.FirstChild("MessageId");
    if(!messageIdNode.IsNull())
    {
      m_messageId = Aws::Utils::Xml::DecodeEscapedXmlText(messageIdNode.GetText());
    }
    XmlNode sequenceNumberNode = resultNode.FirstChild("SequenceNumber");
    if(!sequenceNumberNode.IsNull())
    {
      m_sequenceNumber = Aws::Utils::Xml::DecodeEscapedXmlText(sequenceNumberNode.GetText());
    }
  }

  if (!rootNode.IsNull()) {
    XmlNode responseMetadataNode = rootNode.FirstChild("ResponseMetadata");
    m_responseMetadata = responseMetadataNode;
    AWS_LOGSTREAM_DEBUG("Aws::SQS::Model::SendMessageResult", "x-amzn-request-id: " << m_responseMetadata.GetRequestId() );
  }
  return *this;
}
