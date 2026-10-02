/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/ChangeMessageVisibilityBatchRequestEntry.h>
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

ChangeMessageVisibilityBatchRequestEntry::ChangeMessageVisibilityBatchRequestEntry() : 
    m_idHasBeenSet(false),
    m_receiptHandleHasBeenSet(false),
    m_visibilityTimeout(0),
    m_visibilityTimeoutHasBeenSet(false)
{
}

ChangeMessageVisibilityBatchRequestEntry::ChangeMessageVisibilityBatchRequestEntry(const XmlNode& xmlNode) : 
    m_idHasBeenSet(false),
    m_receiptHandleHasBeenSet(false),
    m_visibilityTimeout(0),
    m_visibilityTimeoutHasBeenSet(false)
{
  *this = xmlNode;
}

ChangeMessageVisibilityBatchRequestEntry& ChangeMessageVisibilityBatchRequestEntry::operator =(const XmlNode& xmlNode)
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
    XmlNode receiptHandleNode = resultNode.FirstChild("ReceiptHandle");
    if(!receiptHandleNode.IsNull())
    {
      m_receiptHandle = Aws::Utils::Xml::DecodeEscapedXmlText(receiptHandleNode.GetText());
      m_receiptHandleHasBeenSet = true;
    }
    XmlNode visibilityTimeoutNode = resultNode.FirstChild("VisibilityTimeout");
    if(!visibilityTimeoutNode.IsNull())
    {
      m_visibilityTimeout = StringUtils::ConvertToInt32(StringUtils::Trim(Aws::Utils::Xml::DecodeEscapedXmlText(visibilityTimeoutNode.GetText()).c_str()).c_str());
      m_visibilityTimeoutHasBeenSet = true;
    }
  }

  return *this;
}

void ChangeMessageVisibilityBatchRequestEntry::OutputToStream(Aws::OStream& oStream, const char* location, unsigned index, const char* locationValue) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << index << locationValue << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }

  if(m_receiptHandleHasBeenSet)
  {
      oStream << location << index << locationValue << ".ReceiptHandle=" << StringUtils::URLEncode(m_receiptHandle.c_str()) << "&";
  }

  if(m_visibilityTimeoutHasBeenSet)
  {
      oStream << location << index << locationValue << ".VisibilityTimeout=" << m_visibilityTimeout << "&";
  }

}

void ChangeMessageVisibilityBatchRequestEntry::OutputToStream(Aws::OStream& oStream, const char* location) const
{
  if(m_idHasBeenSet)
  {
      oStream << location << ".Id=" << StringUtils::URLEncode(m_id.c_str()) << "&";
  }
  if(m_receiptHandleHasBeenSet)
  {
      oStream << location << ".ReceiptHandle=" << StringUtils::URLEncode(m_receiptHandle.c_str()) << "&";
  }
  if(m_visibilityTimeoutHasBeenSet)
  {
      oStream << location << ".VisibilityTimeout=" << m_visibilityTimeout << "&";
  }
}

} // namespace Model
} // namespace SQS
} // namespace Aws
