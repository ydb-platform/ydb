/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 
 
#include <aws/s3/model/CompleteMultipartUploadResult.h> 
#include <aws/core/utils/xml/XmlSerializer.h> 
#include <aws/core/AmazonWebServiceResult.h> 
#include <aws/core/utils/StringUtils.h> 
#include <aws/core/utils/memory/stl/AWSStringStream.h> 
 
#include <utility> 
 
using namespace Aws::S3::Model; 
using namespace Aws::Utils::Xml; 
using namespace Aws::Utils; 
using namespace Aws; 
 
CompleteMultipartUploadResult::CompleteMultipartUploadResult() :  
    m_serverSideEncryption(ServerSideEncryption::NOT_SET), 
    m_bucketKeyEnabled(false), 
    m_requestCharged(RequestCharged::NOT_SET) 
{ 
} 
 
CompleteMultipartUploadResult::CompleteMultipartUploadResult(const Aws::AmazonWebServiceResult<XmlDocument>& result) :  
    m_serverSideEncryption(ServerSideEncryption::NOT_SET), 
    m_bucketKeyEnabled(false), 
    m_requestCharged(RequestCharged::NOT_SET) 
{ 
  *this = result; 
} 
 
CompleteMultipartUploadResult& CompleteMultipartUploadResult::operator =(const Aws::AmazonWebServiceResult<XmlDocument>& result) 
{ 
  const XmlDocument& xmlDocument = result.GetPayload(); 
  XmlNode resultNode = xmlDocument.GetRootElement(); 
 
  if(!resultNode.IsNull()) 
  { 
    XmlNode locationNode = resultNode.FirstChild("Location"); 
    if(!locationNode.IsNull()) 
    { 
      m_location = Aws::Utils::Xml::DecodeEscapedXmlText(locationNode.GetText()); 
    } 
    XmlNode bucketNode = resultNode.FirstChild("Bucket"); 
    if(!bucketNode.IsNull()) 
    { 
      m_bucket = Aws::Utils::Xml::DecodeEscapedXmlText(bucketNode.GetText()); 
    } 
    XmlNode keyNode = resultNode.FirstChild("Key"); 
    if(!keyNode.IsNull()) 
    { 
      m_key = Aws::Utils::Xml::DecodeEscapedXmlText(keyNode.GetText()); 
    } 
    XmlNode eTagNode = resultNode.FirstChild("ETag"); 
    if(!eTagNode.IsNull()) 
    { 
      m_eTag = Aws::Utils::Xml::DecodeEscapedXmlText(eTagNode.GetText()); 
    } 
  } 
 
  const auto& headers = result.GetHeaderValueCollection(); 
  const auto& expirationIter = headers.find("x-amz-expiration"); 
  if(expirationIter != headers.end()) 
  { 
    m_expiration = expirationIter->second; 
  } 
 
  const auto& serverSideEncryptionIter = headers.find("x-amz-server-side-encryption"); 
  if(serverSideEncryptionIter != headers.end()) 
  { 
    m_serverSideEncryption = ServerSideEncryptionMapper::GetServerSideEncryptionForName(serverSideEncryptionIter->second); 
  } 
 
  const auto& versionIdIter = headers.find("x-amz-version-id"); 
  if(versionIdIter != headers.end()) 
  { 
    m_versionId = versionIdIter->second; 
  } 
 
  const auto& sSEKMSKeyIdIter = headers.find("x-amz-server-side-encryption-aws-kms-key-id"); 
  if(sSEKMSKeyIdIter != headers.end()) 
  { 
    m_sSEKMSKeyId = sSEKMSKeyIdIter->second; 
  } 
 
  const auto& bucketKeyEnabledIter = headers.find("x-amz-server-side-encryption-bucket-key-enabled"); 
  if(bucketKeyEnabledIter != headers.end()) 
  { 
     m_bucketKeyEnabled = StringUtils::ConvertToBool(bucketKeyEnabledIter->second.c_str()); 
  } 
 
  const auto& requestChargedIter = headers.find("x-amz-request-charged"); 
  if(requestChargedIter != headers.end()) 
  { 
    m_requestCharged = RequestChargedMapper::GetRequestChargedForName(requestChargedIter->second); 
  } 
 
  return *this; 
} 
