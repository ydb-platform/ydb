/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/SendMessageBatchRequestEntry.h>
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

SendMessageBatchRequestEntry::SendMessageBatchRequestEntry() : 
    m_idHasBeenSet(false),
    m_messageBodyHasBeenSet(false),
    m_delaySeconds(0),
    m_delaySecondsHasBeenSet(false),
    m_messageAttributesHasBeenSet(false),
    m_messageSystemAttributesHasBeenSet(false),
    m_messageDeduplicationIdHasBeenSet(false),
    m_messageGroupIdHasBeenSet(false)
{
}

SendMessageBatchRequestEntry::SendMessageBatchRequestEntry(const XmlNode& xmlNode) : 
    m_idHasBeenSet(false),
    m_messageBodyHasBeenSet(false),
    m_delaySeconds(0),
    m_delaySecondsHasBeenSet(false),
    m_messageAttributesHasBeenSet(false),
    m_messageSystemAttributesHasBeenSet(false),
    m_messageDeduplicationIdHasBeenSet(false),
    m_messageGroupIdHasBeenSet(false)
{
  *this = xmlNode;
}

SendMessageBatchRequestEntry& SendMessageBatchRequestEntry::operator =(const XmlNode& xmlNode)
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
    XmlNode messageBodyNode = resultNode.FirstChild("MessageBody");
    if(!messageBodyNode.IsNull())
    {
      m_messageBody = Aws::Utils::Xml::DecodeEscapedXmlText(messageBodyNode.GetText());
      m_messageBodyHasBeenSet = true;
    }
    XmlNode delaySecondsNode = resultNode.FirstChild("DelaySeconds");
    if(!delaySecondsNode.IsNull())
    {
      m_delaySeconds = StringUtils::ConvertToInt32(StringUtils::Trim(Aws::Utils::Xml::DecodeEscapedXmlText(delaySecondsNode.GetText()).c_str()).c_str());
      m_delaySecondsHasBeenSet = true;
    }
    XmlNode messageAttributesNode = resultNode.FirstChild("MessageAttribute");
    if(!messageAttributesNode.IsNull())
    {
      XmlNode messageAttributeEntry = messageAttributesNode;
      while(!messageAttributeEntry.IsNull())
      {
        XmlNode keyNode = messageAttributeEntry.FirstChild("Name");
        XmlNode valueNode = messageAttributeEntry.FirstChild("Value");
        m_messageAttributes[keyNode.GetText()] =
            valueNode;
        messageAttributeEntry = messageAttributeEntry.NextNode("MessageAttribute");
      }

      m_messageAttributesHasBeenSet = true;
    }
    XmlNode messageSystemAttributesNode = resultNode.FirstChild("MessageSystemAttribute");
    if(!messageSystemAttributesNode.IsNull())
    {
      XmlNode messageSystemAttributeEntry = messageSystemAttributesNode;
      while(!messageSystemAttributeEntry.IsNull())
      {
        XmlNode keyNode = messageSystemAttributeEntry.FirstChild("Name");
        XmlNode valueNode = messageSystemAttributeEntry.FirstChild("Value");
        m_messageSystemAttributes[MessageSystemAttributeNameForSendsMapper::GetMessageSystemAttributeNameForSendsForName(StringUtils::Trim(keyNode.GetText().c_str()))] =
            valueNode;
        messageSystemAttributeEntry = messageSystemAttributeEntry.NextNode("MessageSystemAttribute");
      }

      m_messageSystemAttributesHasBeenSet = true;
    }
    XmlNode messageDeduplicationIdNode = resultNode.FirstChild("MessageDeduplicationId");
    if(!messageDeduplicationIdNode.IsNull())
    {
      m_messageDeduplicationId = Aws::Utils::Xml::DecodeEscapedXmlText(messageDeduplicationIdNode.GetText());
      m_messageDeduplicationIdHasBeenSet = true;
    }
    XmlNode messageGroupIdNode = resultNode.FirstChild("MessageGroupId");
    if(!messageGroupIdNode.IsNull())
    {
      m_messageGroupId = Aws::Utils::Xml::DecodeEscapedXmlText(messageGroupIdNode.GetText());
      m_messageGroupIdHasBeenSet = true;
    }
  }

  return *this;
}

void SendMessageBatchRequestEntry::OutputToStream(Aws::OStream& oStream, const char* location, unsigned index, const char* locationValue) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << index << locationValue << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }

  if(m_messageBodyHasBeenSet)
  {
      oStream << location << index << locationValue << ".MessageBody=" << StringUtils::URLEncode(m_messageBody.c_str()) << "&";
  }

  if(m_delaySecondsHasBeenSet)
  {
      oStream << location << index << locationValue << ".DelaySeconds=" << m_delaySeconds << "&";
  }

  if(m_messageAttributesHasBeenSet)
  {
      unsigned messageAttributesIdx = 1;
      for(auto& item : m_messageAttributes)
      {
        oStream << location << index << locationValue << ".MessageAttribute." << messageAttributesIdx << ".Name="
            << StringUtils::URLEncode(item.first.c_str()) << "&";
        Aws::StringStream messageAttributesSs;
        messageAttributesSs << location << index << locationValue << ".MessageAttribute." << messageAttributesIdx << ".Value";
        item.second.OutputToStream(oStream, messageAttributesSs.str().c_str());
        messageAttributesIdx++;
      }
  }

  if(m_messageSystemAttributesHasBeenSet)
  {
      unsigned messageSystemAttributesIdx = 1;
      for(auto& item : m_messageSystemAttributes)
      {
        oStream << location << index << locationValue << ".MessageSystemAttribute." << messageSystemAttributesIdx << ".Name="
            << StringUtils::URLEncode(MessageSystemAttributeNameForSendsMapper::GetNameForMessageSystemAttributeNameForSends(item.first).c_str()) << "&";
        Aws::StringStream messageSystemAttributesSs;
        messageSystemAttributesSs << location << index << locationValue << ".MessageSystemAttribute." << messageSystemAttributesIdx << ".Value";
        item.second.OutputToStream(oStream, messageSystemAttributesSs.str().c_str());
        messageSystemAttributesIdx++;
      }
  }

  if(m_messageDeduplicationIdHasBeenSet)
  {
      oStream << location << index << locationValue << ".MessageDeduplicationId=" << StringUtils::URLEncode(m_messageDeduplicationId.c_str()) << "&";
  }

  if(m_messageGroupIdHasBeenSet)
  {
      oStream << location << index << locationValue << ".MessageGroupId=" << StringUtils::URLEncode(m_messageGroupId.c_str()) << "&";
  }

}

void SendMessageBatchRequestEntry::OutputToStream(Aws::OStream& oStream, const char* location) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }
  if(m_messageBodyHasBeenSet)
  {
      oStream << location << ".MessageBody=" << StringUtils::URLEncode(m_messageBody.c_str()) << "&";
  }
  if(m_delaySecondsHasBeenSet)
  {
      oStream << location << ".DelaySeconds=" << m_delaySeconds << "&";
  }
  if(m_messageAttributesHasBeenSet)
  {
      unsigned messageAttributesIdx = 1;
      for(auto& item : m_messageAttributes)
      {
        oStream << location << ".MessageAttribute."  << messageAttributesIdx << ".Name="
            << StringUtils::URLEncode(item.first.c_str()) << "&";
        Aws::StringStream messageAttributesSs;
        messageAttributesSs << location << ".MessageAttribute." << messageAttributesIdx << ".Value";
        item.second.OutputToStream(oStream, messageAttributesSs.str().c_str());
        messageAttributesIdx++;
      }

  }
  if(m_messageSystemAttributesHasBeenSet)
  {
      unsigned messageSystemAttributesIdx = 1;
      for(auto& item : m_messageSystemAttributes)
      {
        oStream << location << ".MessageSystemAttribute."  << messageSystemAttributesIdx << ".Name="
            << StringUtils::URLEncode(MessageSystemAttributeNameForSendsMapper::GetNameForMessageSystemAttributeNameForSends(item.first).c_str()) << "&";
        Aws::StringStream messageSystemAttributesSs;
        messageSystemAttributesSs << location << ".MessageSystemAttribute." << messageSystemAttributesIdx << ".Value";
        item.second.OutputToStream(oStream, messageSystemAttributesSs.str().c_str());
        messageSystemAttributesIdx++;
      }

  }
  if(m_messageDeduplicationIdHasBeenSet)
  {
      oStream << location << ".MessageDeduplicationId=" << StringUtils::URLEncode(m_messageDeduplicationId.c_str()) << "&";
  }
  if(m_messageGroupIdHasBeenSet)
  {
      oStream << location << ".MessageGroupId=" << StringUtils::URLEncode(m_messageGroupId.c_str()) << "&";
  }
}

} // namespace Model
} // namespace SQS
} // namespace Aws
