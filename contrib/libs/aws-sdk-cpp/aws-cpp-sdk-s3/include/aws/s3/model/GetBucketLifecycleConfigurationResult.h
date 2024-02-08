/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/LifecycleRule.h>
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
  class GetBucketLifecycleConfigurationResult
  {
  public:
    AWS_S3_API GetBucketLifecycleConfigurationResult();
    AWS_S3_API GetBucketLifecycleConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketLifecycleConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline const Aws::Vector<LifecycleRule>& GetRules() const{ return m_rules; }

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline void SetRules(const Aws::Vector<LifecycleRule>& value) { m_rules = value; }

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline void SetRules(Aws::Vector<LifecycleRule>&& value) { m_rules = std::move(value); }

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline GetBucketLifecycleConfigurationResult& WithRules(const Aws::Vector<LifecycleRule>& value) { SetRules(value); return *this;}

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline GetBucketLifecycleConfigurationResult& WithRules(Aws::Vector<LifecycleRule>&& value) { SetRules(std::move(value)); return *this;}

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline GetBucketLifecycleConfigurationResult& AddRules(const LifecycleRule& value) { m_rules.push_back(value); return *this; }

    /**
     * <p>Container for a lifecycle rule.</p>
     */
    inline GetBucketLifecycleConfigurationResult& AddRules(LifecycleRule&& value) { m_rules.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<LifecycleRule> m_rules;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
