/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageBatchResultEntry.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <utility>

using namespace Aws::Utils::Xml;
using namespace Aws::Utils;

namespace Aws
{
namespace SQS
{
namespace Model
{

SendMessageBatchResultEntry::SendMessageBatchResultEntry() : 
    m_idHasBeenSet(false),
    m_messageIdHasBeenSet(false),
    m_mD5OfMessageBodyHasBeenSet(false),
    m_mD5OfMessageAttributesHasBeenSet(false),
    m_mD5OfMessageSystemAttributesHasBeenSet(false),
    m_sequenceNumberHasBeenSet(false)
{
}

SendMessageBatchResultEntry::SendMessageBatchResultEntry(const XmlNode& xmlNode) : 
    m_idHasBeenSet(false),
    m_messageIdHasBeenSet(false),
    m_mD5OfMessageBodyHasBeenSet(false),
    m_mD5OfMessageAttributesHasBeenSet(false),
    m_mD5OfMessageSystemAttributesHasBeenSet(false),
    m_sequenceNumberHasBeenSet(false)
{
  *this = xmlNode;
}

SendMessageBatchResultEntry& SendMessageBatchResultEntry::operator =(const XmlNode& xmlNode)
{
  XmlNode resultNode = xmlNode;

  if(!resultNode.IsNull())
  {
    XmlNode idNode = resultNode.FirstChild("Id");
    if(!idNode.IsNull())
    {
      m_id = Aws::Utils::Xml::DecodeEscapedXmlText(idNode.GetText());
      m_idHasBeenSet = true;
    }
    XmlNode messageIdNode = resultNode.FirstChild("MessageId");
    if(!messageIdNode.IsNull())
    {
      m_messageId = Aws::Utils::Xml::DecodeEscapedXmlText(messageIdNode.GetText());
      m_messageIdHasBeenSet = true;
    }
    XmlNode mD5OfMessageBodyNode = resultNode.FirstChild("MD5OfMessageBody");
    if(!mD5OfMessageBodyNode.IsNull())
    {
      m_mD5OfMessageBody = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageBodyNode.GetText());
      m_mD5OfMessageBodyHasBeenSet = true;
    }
    XmlNode mD5OfMessageAttributesNode = resultNode.FirstChild("MD5OfMessageAttributes");
    if(!mD5OfMessageAttributesNode.IsNull())
    {
      m_mD5OfMessageAttributes = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageAttributesNode.GetText());
      m_mD5OfMessageAttributesHasBeenSet = true;
    }
    XmlNode mD5OfMessageSystemAttributesNode = resultNode.FirstChild("MD5OfMessageSystemAttributes");
    if(!mD5OfMessageSystemAttributesNode.IsNull())
    {
      m_mD5OfMessageSystemAttributes = Aws::Utils::Xml::DecodeEscapedXmlText(mD5OfMessageSystemAttributesNode.GetText());
      m_mD5OfMessageSystemAttributesHasBeenSet = true;
    }
    XmlNode sequenceNumberNode = resultNode.FirstChild("SequenceNumber");
    if(!sequenceNumberNode.IsNull())
    {
      m_sequenceNumber = Aws::Utils::Xml::DecodeEscapedXmlText(sequenceNumberNode.GetText());
      m_sequenceNumberHasBeenSet = true;
    }
  }

  return *this;
}

void SendMessageBatchResultEntry::OutputToStream(Aws::OStream& oStream, const char* location, unsigned index, const char* locationValue) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << index << locationValue << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }

  if(m_messageIdHasBeenSet)
  {
      oStream << location << index << locationValue << ".MessageId=" << StringUtils::URLEncode(m_messageId.c_str()) << "&";
  }

  if(m_mD5OfMessageBodyHasBeenSet)
  {
      oStream << location << index << locationValue << ".MD5OfMessageBody=" << StringUtils::URLEncode(m_mD5OfMessageBody.c_str()) << "&";
  }

  if(m_mD5OfMessageAttributesHasBeenSet)
  {
      oStream << location << index << locationValue << ".MD5OfMessageAttributes=" << StringUtils::URLEncode(m_mD5OfMessageAttributes.c_str()) << "&";
  }

  if(m_mD5OfMessageSystemAttributesHasBeenSet)
  {
      oStream << location << index << locationValue << ".MD5OfMessageSystemAttributes=" << StringUtils::URLEncode(m_mD5OfMessageSystemAttributes.c_str()) << "&";
  }

  if(m_sequenceNumberHasBeenSet)
  {
      oStream << location << index << locationValue << ".SequenceNumber=" << StringUtils::URLEncode(m_sequenceNumber.c_str()) << "&";
  }

}

void SendMessageBatchResultEntry::OutputToStream(Aws::OStream& oStream, const char* location) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }
  if(m_messageIdHasBeenSet)
  {
      oStream << location << ".MessageId=" << StringUtils::URLEncode(m_messageId.c_str()) << "&";
  }
  if(m_mD5OfMessageBodyHasBeenSet)
  {
      oStream << location << ".MD5OfMessageBody=" << StringUtils::URLEncode(m_mD5OfMessageBody.c_str()) << "&";
  }
  if(m_mD5OfMessageAttributesHasBeenSet)
  {
      oStream << location << ".MD5OfMessageAttributes=" << StringUtils::URLEncode(m_mD5OfMessageAttributes.c_str()) << "&";
  }
  if(m_mD5OfMessageSystemAttributesHasBeenSet)
  {
      oStream << location << ".MD5OfMessageSystemAttributes=" << StringUtils::URLEncode(m_mD5OfMessageSystemAttributes.c_str()) << "&";
  }
  if(m_sequenceNumberHasBeenSet)
  {
      oStream << location << ".SequenceNumber=" << StringUtils::URLEncode(m_sequenceNumber.c_str()) << "&";
  }
}

} // namespace Model
} // namespace SQS
} // namespace Aws
