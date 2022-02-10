/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/model/GetBucketPolicyResult.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/StringUtils.h>

#include <utility>

using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;
using namespace Aws::Utils;
using namespace Aws;

GetBucketPolicyResult::GetBucketPolicyResult()
{
}

GetBucketPolicyResult::GetBucketPolicyResult(GetBucketPolicyResult&& toMove) : 
    m_policy(std::move(toMove.m_policy))
{
}

GetBucketPolicyResult& GetBucketPolicyResult::operator=(GetBucketPolicyResult&& toMove)
{
   if(this == &toMove)
   {
      return *this;
   }

   m_policy = std::move(toMove.m_policy);

   return *this;
}

GetBucketPolicyResult::GetBucketPolicyResult(Aws::AmazonWebServiceResult<ResponseStream>&& result)
{
  *this = std::move(result);
}

GetBucketPolicyResult& GetBucketPolicyResult::operator =(Aws::AmazonWebServiceResult<ResponseStream>&& result)
{
  m_policy = result.TakeOwnershipOfPayload();

   return *this;
}
