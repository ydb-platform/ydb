/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace S3
{
namespace Model
{
  class GetBucketPolicyResult
  {
  public:
    AWS_S3_API GetBucketPolicyResult();
    //We have to define these because Microsoft doesn't auto generate them
    AWS_S3_API GetBucketPolicyResult(GetBucketPolicyResult&&);
    AWS_S3_API GetBucketPolicyResult& operator=(GetBucketPolicyResult&&);
    //we delete these because Microsoft doesn't handle move generation correctly
    //and we therefore don't trust them to get it right here either.
    GetBucketPolicyResult(const GetBucketPolicyResult&) = delete;
    GetBucketPolicyResult& operator=(const GetBucketPolicyResult&) = delete;


    AWS_S3_API GetBucketPolicyResult(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);
    AWS_S3_API GetBucketPolicyResult& operator=(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);



    /**
     * <p>The bucket policy as a JSON document.</p>
     */
    inline Aws::IOStream& GetPolicy() const { return m_policy.GetUnderlyingStream(); }

    /**
     * <p>The bucket policy as a JSON document.</p>
     */
    inline void ReplaceBody(Aws::IOStream* body) { m_policy = Aws::Utils::Stream::ResponseStream(body); }

  private:

    Aws::Utils::Stream::ResponseStream m_policy;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
