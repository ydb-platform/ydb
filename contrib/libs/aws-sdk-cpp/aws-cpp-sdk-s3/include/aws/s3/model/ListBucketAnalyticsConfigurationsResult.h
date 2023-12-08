/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/AnalyticsConfiguration.h>
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
  class ListBucketAnalyticsConfigurationsResult
  {
  public:
    AWS_S3_API ListBucketAnalyticsConfigurationsResult();
    AWS_S3_API ListBucketAnalyticsConfigurationsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API ListBucketAnalyticsConfigurationsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.</p>
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.</p>
     */
    inline void SetIsTruncated(bool value) { m_isTruncated = value; }

    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationToken = value; }

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationToken = std::move(value); }

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline void SetContinuationToken(const char* value) { m_continuationToken.assign(value); }

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * <p>The marker that is used as a starting point for this analytics configuration
     * list response. This value is present if it was sent in the request.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}


    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline const Aws::String& GetNextContinuationToken() const{ return m_nextContinuationToken; }

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline void SetNextContinuationToken(const Aws::String& value) { m_nextContinuationToken = value; }

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline void SetNextContinuationToken(Aws::String&& value) { m_nextContinuationToken = std::move(value); }

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline void SetNextContinuationToken(const char* value) { m_nextContinuationToken.assign(value); }

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(const Aws::String& value) { SetNextContinuationToken(value); return *this;}

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(Aws::String&& value) { SetNextContinuationToken(std::move(value)); return *this;}

    /**
     * <p> <code>NextContinuationToken</code> is sent when <code>isTruncated</code> is
     * true, which indicates that there are more analytics configurations to list. The
     * next request must include this <code>NextContinuationToken</code>. The token is
     * obfuscated and is not a usable value.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(const char* value) { SetNextContinuationToken(value); return *this;}


    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline const Aws::Vector<AnalyticsConfiguration>& GetAnalyticsConfigurationList() const{ return m_analyticsConfigurationList; }

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline void SetAnalyticsConfigurationList(const Aws::Vector<AnalyticsConfiguration>& value) { m_analyticsConfigurationList = value; }

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline void SetAnalyticsConfigurationList(Aws::Vector<AnalyticsConfiguration>&& value) { m_analyticsConfigurationList = std::move(value); }

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithAnalyticsConfigurationList(const Aws::Vector<AnalyticsConfiguration>& value) { SetAnalyticsConfigurationList(value); return *this;}

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& WithAnalyticsConfigurationList(Aws::Vector<AnalyticsConfiguration>&& value) { SetAnalyticsConfigurationList(std::move(value)); return *this;}

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& AddAnalyticsConfigurationList(const AnalyticsConfiguration& value) { m_analyticsConfigurationList.push_back(value); return *this; }

    /**
     * <p>The list of analytics configurations for a bucket.</p>
     */
    inline ListBucketAnalyticsConfigurationsResult& AddAnalyticsConfigurationList(AnalyticsConfiguration&& value) { m_analyticsConfigurationList.push_back(std::move(value)); return *this; }

  private:

    bool m_isTruncated;

    Aws::String m_continuationToken;

    Aws::String m_nextContinuationToken;

    Aws::Vector<AnalyticsConfiguration> m_analyticsConfigurationList;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
