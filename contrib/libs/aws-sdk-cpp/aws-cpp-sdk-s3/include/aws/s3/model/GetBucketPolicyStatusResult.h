/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/PolicyStatus.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class GetBucketPolicyStatusResult
  {
  public:
    AWS_S3_API GetBucketPolicyStatusResult();
    AWS_S3_API GetBucketPolicyStatusResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketPolicyStatusResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline const PolicyStatus& GetPolicyStatus() const{ return m_policyStatus; }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline void SetPolicyStatus(const PolicyStatus& value) { m_policyStatus = value; }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline void SetPolicyStatus(PolicyStatus&& value) { m_policyStatus = std::move(value); }

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline GetBucketPolicyStatusResult& WithPolicyStatus(const PolicyStatus& value) { SetPolicyStatus(value); return *this;}

    /**
     * <p>The policy status for the specified bucket.</p>
     */
    inline GetBucketPolicyStatusResult& WithPolicyStatus(PolicyStatus&& value) { SetPolicyStatus(std::move(value)); return *this;}

  private:

    PolicyStatus m_policyStatus;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
