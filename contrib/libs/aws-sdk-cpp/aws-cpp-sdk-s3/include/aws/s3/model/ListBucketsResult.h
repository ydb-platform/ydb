/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Owner.h>
#include <aws/s3/model/Bucket.h>
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
  class ListBucketsResult
  {
  public:
    AWS_S3_API ListBucketsResult();
    AWS_S3_API ListBucketsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API ListBucketsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline const Aws::Vector<Bucket>& GetBuckets() const{ return m_buckets; }

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline void SetBuckets(const Aws::Vector<Bucket>& value) { m_buckets = value; }

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline void SetBuckets(Aws::Vector<Bucket>&& value) { m_buckets = std::move(value); }

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline ListBucketsResult& WithBuckets(const Aws::Vector<Bucket>& value) { SetBuckets(value); return *this;}

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline ListBucketsResult& WithBuckets(Aws::Vector<Bucket>&& value) { SetBuckets(std::move(value)); return *this;}

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline ListBucketsResult& AddBuckets(const Bucket& value) { m_buckets.push_back(value); return *this; }

    /**
     * <p>The list of buckets owned by the requester.</p>
     */
    inline ListBucketsResult& AddBuckets(Bucket&& value) { m_buckets.push_back(std::move(value)); return *this; }


    /**
     * <p>The owner of the buckets listed.</p>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p>The owner of the buckets listed.</p>
     */
    inline void SetOwner(const Owner& value) { m_owner = value; }

    /**
     * <p>The owner of the buckets listed.</p>
     */
    inline void SetOwner(Owner&& value) { m_owner = std::move(value); }

    /**
     * <p>The owner of the buckets listed.</p>
     */
    inline ListBucketsResult& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p>The owner of the buckets listed.</p>
     */
    inline ListBucketsResult& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}

  private:

    Aws::Vector<Bucket> m_buckets;

    Owner m_owner;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
