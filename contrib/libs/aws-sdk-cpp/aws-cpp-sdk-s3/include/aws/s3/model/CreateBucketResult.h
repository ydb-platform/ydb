/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
  class CreateBucketResult
  {
  public:
    AWS_S3_API CreateBucketResult();
    AWS_S3_API CreateBucketResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API CreateBucketResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline const Aws::String& GetLocation() const{ return m_location; }

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline void SetLocation(const Aws::String& value) { m_location = value; }

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline void SetLocation(Aws::String&& value) { m_location = std::move(value); }

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline void SetLocation(const char* value) { m_location.assign(value); }

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline CreateBucketResult& WithLocation(const Aws::String& value) { SetLocation(value); return *this;}

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline CreateBucketResult& WithLocation(Aws::String&& value) { SetLocation(std::move(value)); return *this;}

    /**
     * <p>A forward slash followed by the name of the bucket.</p>
     */
    inline CreateBucketResult& WithLocation(const char* value) { SetLocation(value); return *this;}

  private:

    Aws::String m_location;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
