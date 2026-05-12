/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/BatchResultErrorEntry.h>
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

BatchResultErrorEntry::BatchResultErrorEntry() : 
    m_idHasBeenSet(false),
    m_senderFault(false),
    m_senderFaultHasBeenSet(false),
    m_codeHasBeenSet(false),
    m_messageHasBeenSet(false)
{
}

BatchResultErrorEntry::BatchResultErrorEntry(const XmlNode& xmlNode) : 
    m_idHasBeenSet(false),
    m_senderFault(false),
    m_senderFaultHasBeenSet(false),
    m_codeHasBeenSet(false),
    m_messageHasBeenSet(false)
{
  *this = xmlNode;
}

BatchResultErrorEntry& BatchResultErrorEntry::operator =(const XmlNode& xmlNode)
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
    XmlNode senderFaultNode = resultNode.FirstChild("SenderFault");
    if(!senderFaultNode.IsNull())
    {
      m_senderFault = StringUtils::ConvertToBool(StringUtils::Trim(Aws::Utils::Xml::DecodeEscapedXmlText(senderFaultNode.GetText()).c_str()).c_str());
      m_senderFaultHasBeenSet = true;
    }
    XmlNode codeNode = resultNode.FirstChild("Code");
    if(!codeNode.IsNull())
    {
      m_code = Aws::Utils::Xml::DecodeEscapedXmlText(codeNode.GetText());
      m_codeHasBeenSet = true;
    }
    XmlNode messageNode = resultNode.FirstChild("Message");
    if(!messageNode.IsNull())
    {
      m_message = Aws::Utils::Xml::DecodeEscapedXmlText(messageNode.GetText());
      m_messageHasBeenSet = true;
    }
  }

  return *this;
}

void BatchResultErrorEntry::OutputToStream(Aws::OStream& oStream, const char* location, unsigned index, const char* locationValue) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << index << locationValue << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }

  if(m_senderFaultHasBeenSet)
  {
      oStream << location << index << locationValue << ".SenderFault=" << std::boolalpha << m_senderFault << "&";
  }

  if(m_codeHasBeenSet)
  {
      oStream << location << index << locationValue << ".Code=" << StringUtils::URLEncode(m_code.c_str()) << "&";
  }

  if(m_messageHasBeenSet)
  {
      oStream << location << index << locationValue << ".Message=" << StringUtils::URLEncode(m_message.c_str()) << "&";
  }

}

void BatchResultErrorEntry::OutputToStream(Aws::OStream& oStream, const char* location) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }
  if(m_senderFaultHasBeenSet)
  {
      oStream << location << ".SenderFault=" << std::boolalpha << m_senderFault << "&";
  }
  if(m_codeHasBeenSet)
  {
      oStream << location << ".Code=" << StringUtils::URLEncode(m_code.c_str()) << "&";
  }
  if(m_messageHasBeenSet)
  {
      oStream << location << ".Message=" << StringUtils::URLEncode(m_message.c_str()) << "&";
  }
}

} // namespace Model
} // namespace SQS
} // namespace Aws
