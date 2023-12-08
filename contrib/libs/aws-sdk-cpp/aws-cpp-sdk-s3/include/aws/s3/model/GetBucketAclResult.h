/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/Owner.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Grant.h>
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
  class GetBucketAclResult
  {
  public:
    AWS_S3_API GetBucketAclResult();
    AWS_S3_API GetBucketAclResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketAclResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Container for the bucket owner's display name and ID.</p>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p>Container for the bucket owner's display name and ID.</p>
     */
    inline void SetOwner(const Owner& value) { m_owner = value; }

    /**
     * <p>Container for the bucket owner's display name and ID.</p>
     */
    inline void SetOwner(Owner&& value) { m_owner = std::move(value); }

    /**
     * <p>Container for the bucket owner's display name and ID.</p>
     */
    inline GetBucketAclResult& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p>Container for the bucket owner's display name and ID.</p>
     */
    inline GetBucketAclResult& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * <p>A list of grants.</p>
     */
    inline const Aws::Vector<Grant>& GetGrants() const{ return m_grants; }

    /**
     * <p>A list of grants.</p>
     */
    inline void SetGrants(const Aws::Vector<Grant>& value) { m_grants = value; }

    /**
     * <p>A list of grants.</p>
     */
    inline void SetGrants(Aws::Vector<Grant>&& value) { m_grants = std::move(value); }

    /**
     * <p>A list of grants.</p>
     */
    inline GetBucketAclResult& WithGrants(const Aws::Vector<Grant>& value) { SetGrants(value); return *this;}

    /**
     * <p>A list of grants.</p>
     */
    inline GetBucketAclResult& WithGrants(Aws::Vector<Grant>&& value) { SetGrants(std::move(value)); return *this;}

    /**
     * <p>A list of grants.</p>
     */
    inline GetBucketAclResult& AddGrants(const Grant& value) { m_grants.push_back(value); return *this; }

    /**
     * <p>A list of grants.</p>
     */
    inline GetBucketAclResult& AddGrants(Grant&& value) { m_grants.push_back(std::move(value)); return *this; }

  private:

    Owner m_owner;

    Aws::Vector<Grant> m_grants;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
