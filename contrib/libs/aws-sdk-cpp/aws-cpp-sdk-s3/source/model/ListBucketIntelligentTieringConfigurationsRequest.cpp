/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/model/ListBucketIntelligentTieringConfigurationsRequest.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <utility>

using namespace Aws::S3::Model;
using namespace Aws::Utils::Xml;
using namespace Aws::Utils;
using namespace Aws::Http;

ListBucketIntelligentTieringConfigurationsRequest::ListBucketIntelligentTieringConfigurationsRequest() : 
    m_bucketHasBeenSet(false),
    m_continuationTokenHasBeenSet(false),
    m_customizedAccessLogTagHasBeenSet(false)
{
}

Aws::String ListBucketIntelligentTieringConfigurationsRequest::SerializePayload() const
{
  return {};
}

void ListBucketIntelligentTieringConfigurationsRequest::AddQueryStringParameters(URI& uri) const
{
    Aws::StringStream ss;
    if(m_continuationTokenHasBeenSet)
    {
      ss << m_continuationToken;
      uri.AddQueryStringParameter("continuation-token", ss.str());
      ss.str("");
    }

    if(!m_customizedAccessLogTag.empty())
    {
        // only accept customized LogTag which starts with "x-"
        Aws::Map<Aws::String, Aws::String> collectedLogTags;
        for(const auto& entry: m_customizedAccessLogTag)
        {
            if (!entry.first.empty() && !entry.second.empty() && entry.first.substr(0, 2) == "x-")
            {
                collectedLogTags.emplace(entry.first, entry.second);
            }
        }

        if (!collectedLogTags.empty())
        {
            uri.AddQueryStringParameter(collectedLogTags);
        }
    }
}


ListBucketIntelligentTieringConfigurationsRequest::EndpointParameters ListBucketIntelligentTieringConfigurationsRequest::GetEndpointContextParams() const
{
    EndpointParameters parameters;
    // Operation context parameters
    if (BucketHasBeenSet()) {
        parameters.emplace_back(Aws::String("Bucket"), this->GetBucket(), Aws::Endpoint::EndpointParameter::ParameterOrigin::OPERATION_CONTEXT);
    }
    return parameters;
}
