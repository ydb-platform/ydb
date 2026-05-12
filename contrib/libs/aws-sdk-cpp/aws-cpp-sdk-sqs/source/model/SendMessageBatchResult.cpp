/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageBatchResult.h>
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

SendMessageBatchResult::SendMessageBatchResult()
{
}

SendMessageBatchResult::SendMessageBatchResult(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  *this = result;
}

SendMessageBatchResult& SendMessageBatchResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result)
{
  const XmlDocument& xmlDocument = result.GetPayload();
  XmlNode rootNode = xmlDocument.GetRootElement();
  XmlNode resultNode = rootNode;
  if (!rootNode.IsNull() && (rootNode.GetName() != "SendMessageBatchResult"))
  {
    resultNode = rootNode.FirstChild("SendMessageBatchResult");
  }

  if(!resultNode.IsNull())
  {
    XmlNode successfulNode = resultNode.FirstChild("SendMessageBatchResultEntry");
    if(!successfulNode.IsNull())
    {
      XmlNode sendMessageBatchResultEntryMember = successfulNode;
      while(!sendMessageBatchResultEntryMember.IsNull())
      {
        m_successful.push_back(sendMessageBatchResultEntryMember);
        sendMessageBatchResultEntryMember = sendMessageBatchResultEntryMember.NextNode("SendMessageBatchResultEntry");
      }

    }
    XmlNode failedNode = resultNode.FirstChild("BatchResultErrorEntry");
    if(!failedNode.IsNull())
    {
      XmlNode batchResultErrorEntryMember = failedNode;
      while(!batchResultErrorEntryMember.IsNull())
      {
        m_failed.push_back(batchResultErrorEntryMember);
        batchResultErrorEntryMember = batchResultErrorEntryMember.NextNode("BatchResultErrorEntry");
      }

    }
  }

  if (!rootNode.IsNull()) {
    XmlNode responseMetadataNode = rootNode.FirstChild("ResponseMetadata");
    m_responseMetadata = responseMetadataNode;
    AWS_LOGSTREAM_DEBUG("Aws::SQS::Model::SendMessageBatchResult", "x-amzn-request-id: " << m_responseMetadata.GetRequestId() );
  }
  return *this;
}
