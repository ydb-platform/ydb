/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/MetricsConfiguration.h>
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
  class GetBucketMetricsConfigurationResult
  {
  public:
    AWS_S3_API GetBucketMetricsConfigurationResult();
    AWS_S3_API GetBucketMetricsConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketMetricsConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Specifies the metrics configuration.</p>
     */
    inline const MetricsConfiguration& GetMetricsConfiguration() const{ return m_metricsConfiguration; }

    /**
     * <p>Specifies the metrics configuration.</p>
     */
    inline void SetMetricsConfiguration(const MetricsConfiguration& value) { m_metricsConfiguration = value; }

    /**
     * <p>Specifies the metrics configuration.</p>
     */
    inline void SetMetricsConfiguration(MetricsConfiguration&& value) { m_metricsConfiguration = std::move(value); }

    /**
     * <p>Specifies the metrics configuration.</p>
     */
    inline GetBucketMetricsConfigurationResult& WithMetricsConfiguration(const MetricsConfiguration& value) { SetMetricsConfiguration(value); return *this;}

    /**
     * <p>Specifies the metrics configuration.</p>
     */
    inline GetBucketMetricsConfigurationResult& WithMetricsConfiguration(MetricsConfiguration&& value) { SetMetricsConfiguration(std::move(value)); return *this;}

  private:

    MetricsConfiguration m_metricsConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
