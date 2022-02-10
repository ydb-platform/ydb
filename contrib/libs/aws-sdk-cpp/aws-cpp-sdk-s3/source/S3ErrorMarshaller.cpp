/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/client/AWSError.h>
#include <aws/s3/S3ErrorMarshaller.h>
#include <aws/s3/S3Errors.h>

using namespace Aws::Client;
using namespace Aws::S3;

AWSError<CoreErrors> S3ErrorMarshaller::FindErrorByName(const char* errorName) const
{
  AWSError<CoreErrors> error = S3ErrorMapper::GetErrorForName(errorName);
  if(error.GetErrorType() != CoreErrors::UNKNOWN)
  {
    return error;
  }

  return AWSErrorMarshaller::FindErrorByName(errorName);
}

Aws::String S3ErrorMarshaller::ExtractRegion(const AWSError<CoreErrors>& error) const
{
  const auto& headers = error.GetResponseHeaders();
  const auto& iter = headers.find("x-amz-bucket-region");
  if (iter != headers.end())
  {
    return iter->second;
  }

  const Aws::Utils::Xml::XmlDocument& xmlDocument = GetXmlPayloadFromError(error);
  Aws::Utils::Xml::XmlNode xmlNode = xmlDocument.GetRootElement();
  if (!xmlNode.IsNull())
  {
    Aws::Utils::Xml::XmlNode regionNode = xmlNode.FirstChild("Region");
    if (!regionNode.IsNull())
    {
      return regionNode.GetText().c_str();
    }
  }

  // as a last choice, try finding region from endpoint.
  const auto& locIter = headers.find("location");
  if (locIter != headers.end())
  {
    Aws::Http::URI uri(locIter->second);
    auto authority = uri.GetAuthority();
    // virtual address example: <bucketname>.<[s3-]region>.amazonaws.com
    // path style example: <[s3]-region>.amazonaws.com/<bucketname>
    auto pos = authority.find(".amazonaws.com");
    if (pos == 0 || pos == std::string::npos)
    {
      return {};
    }
    auto endPos = pos - 1;
    while (pos > 0 && authority[pos - 1] != '.')
    {
      pos--;
    }
    auto region = authority.substr(pos, endPos + 1 - pos);
    if (region.compare(0, 3, "s3-") == 0)
    {
        region = region.substr(3);
    }
    if (region.compare(0, 5, "fips-") == 0)
    {
        region = region.substr(5);
    }
    return region;
  }
  return {};
}

Aws::String S3ErrorMarshaller::ExtractEndpoint(const AWSError<CoreErrors>& error) const
{
  const auto& headers = error.GetResponseHeaders();
  const auto& iter = headers.find("location");
  if (iter != headers.end())
  {
    Aws::Http::URI uri(iter->second);
    return uri.GetAuthority();
  }

  const Aws::Utils::Xml::XmlDocument& xmlDocument = GetXmlPayloadFromError(error);
  Aws::Utils::Xml::XmlNode xmlNode = xmlDocument.GetRootElement();
  if (!xmlNode.IsNull())
  {
    Aws::Utils::Xml::XmlNode endpointNode = xmlNode.FirstChild("Endpoint");
    if (!endpointNode.IsNull())
    {
      Aws::Http::URI uri(endpointNode.GetText().c_str());
      return uri.GetAuthority();
    }
  }

  return {};
}