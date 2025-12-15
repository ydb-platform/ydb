/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/ReceiveMessageResult.h>
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

ReceiveMessageResult::ReceiveMessageResult()
{
}

ReceiveMessageResult::ReceiveMessageResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

ReceiveMessageResult& ReceiveMessageResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode rootNode = xmlDocument.GetRootElement();
  XmlNode resultNode = rootNode;
  if (!rootNode.IsNull() && (rootNode.GetName() != "ReceiveMessageResult"))
  {
    resultNode = rootNode.FirstChild("ReceiveMessageResult");
  }

  if(!resultNode.IsNull())
  {
    XmlNode messagesNode = resultNode.FirstChild("Message");
    if(!messagesNode.IsNull())
    {
      XmlNode messageMember = messagesNode;
      while(!messageMember.IsNull())
      {
        m_messages.push_back(messageMember);
        messageMember = messageMember.NextNode("Message");
      }

    }
  }

  if (!rootNode.IsNull()) {
    XmlNode responseMetadataNode = rootNode.FirstChild("ResponseMetadata");
    m_responseMetadata = responseMetadataNode;
    AWS_LOGSTREAM_DEBUG("Aws::SQS::Model::ReceiveMessageResult", "x-amzn-request-id: " << m_responseMetadata.GetRequestId() );
  }
  return *this;
}
