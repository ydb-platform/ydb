/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/ObjectLockRetention.h>
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
  class GetObjectRetentionResult
  {
  public:
    AWS_S3_API GetObjectRetentionResult();
    AWS_S3_API GetObjectRetentionResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetObjectRetentionResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The container element for an object's retention settings.</p>
     */
    inline const ObjectLockRetention& GetRetention() const{ return m_retention; }

    /**
     * <p>The container element for an object's retention settings.</p>
     */
    inline void SetRetention(const ObjectLockRetention& value) { m_retention = value; }

    /**
     * <p>The container element for an object's retention settings.</p>
     */
    inline void SetRetention(ObjectLockRetention&& value) { m_retention = std::move(value); }

    /**
     * <p>The container element for an object's retention settings.</p>
     */
    inline GetObjectRetentionResult& WithRetention(const ObjectLockRetention& value) { SetRetention(value); return *this;}

    /**
     * <p>The container element for an object's retention settings.</p>
     */
    inline GetObjectRetentionResult& WithRetention(ObjectLockRetention&& value) { SetRetention(std::move(value)); return *this;}

  private:

    ObjectLockRetention m_retention;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
