/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/ObjectLockConfiguration.h>
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
  class GetObjectLockConfigurationResult
  {
  public:
    AWS_S3_API GetObjectLockConfigurationResult();
    AWS_S3_API GetObjectLockConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetObjectLockConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The specified bucket's Object Lock configuration.</p>
     */
    inline const ObjectLockConfiguration& GetObjectLockConfiguration() const{ return m_objectLockConfiguration; }

    /**
     * <p>The specified bucket's Object Lock configuration.</p>
     */
    inline void SetObjectLockConfiguration(const ObjectLockConfiguration& value) { m_objectLockConfiguration = value; }

    /**
     * <p>The specified bucket's Object Lock configuration.</p>
     */
    inline void SetObjectLockConfiguration(ObjectLockConfiguration&& value) { m_objectLockConfiguration = std::move(value); }

    /**
     * <p>The specified bucket's Object Lock configuration.</p>
     */
    inline GetObjectLockConfigurationResult& WithObjectLockConfiguration(const ObjectLockConfiguration& value) { SetObjectLockConfiguration(value); return *this;}

    /**
     * <p>The specified bucket's Object Lock configuration.</p>
     */
    inline GetObjectLockConfigurationResult& WithObjectLockConfiguration(ObjectLockConfiguration&& value) { SetObjectLockConfiguration(std::move(value)); return *this;}

  private:

    ObjectLockConfiguration m_objectLockConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
