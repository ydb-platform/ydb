/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/IntelligentTieringConfiguration.h>
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
  class ListBucketIntelligentTieringConfigurationsResult
  {
  public:
    AWS_S3_API ListBucketIntelligentTieringConfigurationsResult();
    AWS_S3_API ListBucketIntelligentTieringConfigurationsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API ListBucketIntelligentTieringConfigurationsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of <code>true</code> indicates that the list is not complete and the
     * <code>NextContinuationToken</code> will be provided for a subsequent
     * request.</p>
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of <code>true</code> indicates that the list is not complete and the
     * <code>NextContinuationToken</code> will be provided for a subsequent
     * request.</p>
     */
    inline void SetIsTruncated(bool value) { m_isTruncated = value; }

    /**
     * <p>Indicates whether the returned list of analytics configurations is complete.
     * A value of <code>true</code> indicates that the list is not complete and the
     * <code>NextContinuationToken</code> will be provided for a subsequent
     * request.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationToken = value; }

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationToken = std::move(value); }

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline void SetContinuationToken(const char* value) { m_continuationToken.assign(value); }

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * <p>The <code>ContinuationToken</code> that represents a placeholder from where
     * this request should begin.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}


    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline const Aws::String& GetNextContinuationToken() const{ return m_nextContinuationToken; }

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline void SetNextContinuationToken(const Aws::String& value) { m_nextContinuationToken = value; }

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline void SetNextContinuationToken(Aws::String&& value) { m_nextContinuationToken = std::move(value); }

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline void SetNextContinuationToken(const char* value) { m_nextContinuationToken.assign(value); }

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithNextContinuationToken(const Aws::String& value) { SetNextContinuationToken(value); return *this;}

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithNextContinuationToken(Aws::String&& value) { SetNextContinuationToken(std::move(value)); return *this;}

    /**
     * <p>The marker used to continue this inventory configuration listing. Use the
     * <code>NextContinuationToken</code> from this response to continue the listing in
     * a subsequent request. The continuation token is an opaque value that Amazon S3
     * understands.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithNextContinuationToken(const char* value) { SetNextContinuationToken(value); return *this;}


    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline const Aws::Vector<IntelligentTieringConfiguration>& GetIntelligentTieringConfigurationList() const{ return m_intelligentTieringConfigurationList; }

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline void SetIntelligentTieringConfigurationList(const Aws::Vector<IntelligentTieringConfiguration>& value) { m_intelligentTieringConfigurationList = value; }

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline void SetIntelligentTieringConfigurationList(Aws::Vector<IntelligentTieringConfiguration>&& value) { m_intelligentTieringConfigurationList = std::move(value); }

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithIntelligentTieringConfigurationList(const Aws::Vector<IntelligentTieringConfiguration>& value) { SetIntelligentTieringConfigurationList(value); return *this;}

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& WithIntelligentTieringConfigurationList(Aws::Vector<IntelligentTieringConfiguration>&& value) { SetIntelligentTieringConfigurationList(std::move(value)); return *this;}

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& AddIntelligentTieringConfigurationList(const IntelligentTieringConfiguration& value) { m_intelligentTieringConfigurationList.push_back(value); return *this; }

    /**
     * <p>The list of S3 Intelligent-Tiering configurations for a bucket.</p>
     */
    inline ListBucketIntelligentTieringConfigurationsResult& AddIntelligentTieringConfigurationList(IntelligentTieringConfiguration&& value) { m_intelligentTieringConfigurationList.push_back(std::move(value)); return *this; }

  private:

    bool m_isTruncated;

    Aws::String m_continuationToken;

    Aws::String m_nextContinuationToken;

    Aws::Vector<IntelligentTieringConfiguration> m_intelligentTieringConfigurationList;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
