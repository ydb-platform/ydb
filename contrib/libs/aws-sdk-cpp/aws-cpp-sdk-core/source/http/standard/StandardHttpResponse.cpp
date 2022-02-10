/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 

#include <aws/core/http/standard/StandardHttpResponse.h>

#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/AWSMemory.h>

#include <istream>

using namespace Aws::Http;
using namespace Aws::Http::Standard;
using namespace Aws::Utils;


HeaderValueCollection StandardHttpResponse::GetHeaders() const
{
    HeaderValueCollection headerValueCollection;

    for (Aws::Map<Aws::String, Aws::String>::const_iterator iter = headerMap.begin(); iter != headerMap.end(); ++iter)
    {
        headerValueCollection.emplace(HeaderValuePair(iter->first, iter->second));
    }

    return headerValueCollection;
}

bool StandardHttpResponse::HasHeader(const char* headerName) const
{
    return headerMap.find(StringUtils::ToLower(headerName)) != headerMap.end();
}

const Aws::String& StandardHttpResponse::GetHeader(const Aws::String& headerName) const
{
    Aws::Map<Aws::String, Aws::String>::const_iterator foundValue = headerMap.find(StringUtils::ToLower(headerName.c_str()));
    return foundValue->second;
}

void StandardHttpResponse::AddHeader(const Aws::String& headerName, const Aws::String& headerValue)
{
    headerMap[StringUtils::ToLower(headerName.c_str())] = headerValue;
}


