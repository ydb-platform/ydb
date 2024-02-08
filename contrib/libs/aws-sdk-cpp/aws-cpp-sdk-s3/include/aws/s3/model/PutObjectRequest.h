/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/s3/model/ObjectCannedACL.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/ChecksumAlgorithm.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/RequestPayer.h>
#include <aws/s3/model/ObjectLockMode.h>
#include <aws/s3/model/ObjectLockLegalHoldStatus.h>
#include <utility>

namespace Aws
{
namespace Http
{
    class URI;
} //namespace Http
namespace S3
{
namespace Model
{

  /**
   */
  class PutObjectRequest : public StreamingS3Request
  {
  public:
    AWS_S3_API PutObjectRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutObject"; }

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    AWS_S3_API Aws::String GetChecksumAlgorithmName() const override;

    /**
     * Helper function to collect parameters (configurable and static hardcoded) required for endpoint computation.
     */
    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline const ObjectCannedACL& GetACL() const{ return m_aCL; }

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline bool ACLHasBeenSet() const { return m_aCLHasBeenSet; }

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetACL(const ObjectCannedACL& value) { m_aCLHasBeenSet = true; m_aCL = value; }

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetACL(ObjectCannedACL&& value) { m_aCLHasBeenSet = true; m_aCL = std::move(value); }

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithACL(const ObjectCannedACL& value) { SetACL(value); return *this;}

    /**
     * <p>The canned ACL to apply to the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL">Canned
     * ACL</a>.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithACL(ObjectCannedACL&& value) { SetACL(std::move(value)); return *this;}


    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The bucket name to which the PUT action was initiated. </p> <p>When using
     * this action with an access point, you must direct requests to the access point
     * hostname. The access point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using this
     * action with Amazon S3 on Outposts, you must direct requests to the S3 on
     * Outposts hostname. The S3 on Outposts hostname takes the form <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline const Aws::String& GetCacheControl() const{ return m_cacheControl; }

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline bool CacheControlHasBeenSet() const { return m_cacheControlHasBeenSet; }

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline void SetCacheControl(const Aws::String& value) { m_cacheControlHasBeenSet = true; m_cacheControl = value; }

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline void SetCacheControl(Aws::String&& value) { m_cacheControlHasBeenSet = true; m_cacheControl = std::move(value); }

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline void SetCacheControl(const char* value) { m_cacheControlHasBeenSet = true; m_cacheControl.assign(value); }

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline PutObjectRequest& WithCacheControl(const Aws::String& value) { SetCacheControl(value); return *this;}

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline PutObjectRequest& WithCacheControl(Aws::String&& value) { SetCacheControl(std::move(value)); return *this;}

    /**
     * <p> Can be used to specify caching behavior along the request/reply chain. For
     * more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9</a>.</p>
     */
    inline PutObjectRequest& WithCacheControl(const char* value) { SetCacheControl(value); return *this;}


    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline const Aws::String& GetContentDisposition() const{ return m_contentDisposition; }

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline bool ContentDispositionHasBeenSet() const { return m_contentDispositionHasBeenSet; }

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline void SetContentDisposition(const Aws::String& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = value; }

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline void SetContentDisposition(Aws::String&& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = std::move(value); }

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline void SetContentDisposition(const char* value) { m_contentDispositionHasBeenSet = true; m_contentDisposition.assign(value); }

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline PutObjectRequest& WithContentDisposition(const Aws::String& value) { SetContentDisposition(value); return *this;}

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline PutObjectRequest& WithContentDisposition(Aws::String&& value) { SetContentDisposition(std::move(value)); return *this;}

    /**
     * <p>Specifies presentational information for the object. For more information,
     * see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1</a>.</p>
     */
    inline PutObjectRequest& WithContentDisposition(const char* value) { SetContentDisposition(value); return *this;}


    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline const Aws::String& GetContentEncoding() const{ return m_contentEncoding; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline bool ContentEncodingHasBeenSet() const { return m_contentEncodingHasBeenSet; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline void SetContentEncoding(const Aws::String& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = value; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline void SetContentEncoding(Aws::String&& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = std::move(value); }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline void SetContentEncoding(const char* value) { m_contentEncodingHasBeenSet = true; m_contentEncoding.assign(value); }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline PutObjectRequest& WithContentEncoding(const Aws::String& value) { SetContentEncoding(value); return *this;}

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline PutObjectRequest& WithContentEncoding(Aws::String&& value) { SetContentEncoding(std::move(value)); return *this;}

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11</a>.</p>
     */
    inline PutObjectRequest& WithContentEncoding(const char* value) { SetContentEncoding(value); return *this;}


    /**
     * <p>The language the content is in.</p>
     */
    inline const Aws::String& GetContentLanguage() const{ return m_contentLanguage; }

    /**
     * <p>The language the content is in.</p>
     */
    inline bool ContentLanguageHasBeenSet() const { return m_contentLanguageHasBeenSet; }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(const Aws::String& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = value; }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(Aws::String&& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = std::move(value); }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(const char* value) { m_contentLanguageHasBeenSet = true; m_contentLanguage.assign(value); }

    /**
     * <p>The language the content is in.</p>
     */
    inline PutObjectRequest& WithContentLanguage(const Aws::String& value) { SetContentLanguage(value); return *this;}

    /**
     * <p>The language the content is in.</p>
     */
    inline PutObjectRequest& WithContentLanguage(Aws::String&& value) { SetContentLanguage(std::move(value)); return *this;}

    /**
     * <p>The language the content is in.</p>
     */
    inline PutObjectRequest& WithContentLanguage(const char* value) { SetContentLanguage(value); return *this;}


    /**
     * <p>Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>.</p>
     */
    inline long long GetContentLength() const{ return m_contentLength; }

    /**
     * <p>Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>.</p>
     */
    inline bool ContentLengthHasBeenSet() const { return m_contentLengthHasBeenSet; }

    /**
     * <p>Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>.</p>
     */
    inline void SetContentLength(long long value) { m_contentLengthHasBeenSet = true; m_contentLength = value; }

    /**
     * <p>Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically. For more information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.13</a>.</p>
     */
    inline PutObjectRequest& WithContentLength(long long value) { SetContentLength(value); return *this;}


    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline bool ContentMD5HasBeenSet() const { return m_contentMD5HasBeenSet; }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline PutObjectRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline PutObjectRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the message (without the headers)
     * according to RFC 1864. This header can be used as a message integrity check to
     * verify that the data is the same data that was originally sent. Although it is
     * optional, we recommend using the Content-MD5 mechanism as an end-to-end
     * integrity check. For more information about REST request authentication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html">REST
     * Authentication</a>.</p>
     */
    inline PutObjectRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline const ChecksumAlgorithm& GetChecksumAlgorithm() const{ return m_checksumAlgorithm; }

    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline bool ChecksumAlgorithmHasBeenSet() const { return m_checksumAlgorithmHasBeenSet; }

    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline void SetChecksumAlgorithm(const ChecksumAlgorithm& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = value; }

    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline void SetChecksumAlgorithm(ChecksumAlgorithm&& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = std::move(value); }

    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline PutObjectRequest& WithChecksumAlgorithm(const ChecksumAlgorithm& value) { SetChecksumAlgorithm(value); return *this;}

    /**
     * <p>Indicates the algorithm used to create the checksum for the object when using
     * the SDK. This header will not provide any additional functionality if not using
     * the SDK. When sending this header, there must be a corresponding
     * <code>x-amz-checksum</code> or <code>x-amz-trailer</code> header sent.
     * Otherwise, Amazon S3 fails the request with the HTTP status code <code>400 Bad
     * Request</code>. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>If you provide
     * an individual checksum, Amazon S3 ignores any provided
     * <code>ChecksumAlgorithm</code> parameter.</p>
     */
    inline PutObjectRequest& WithChecksumAlgorithm(ChecksumAlgorithm&& value) { SetChecksumAlgorithm(std::move(value)); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumCRC32() const{ return m_checksumCRC32; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool ChecksumCRC32HasBeenSet() const { return m_checksumCRC32HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const Aws::String& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(Aws::String&& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const char* value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32(const Aws::String& value) { SetChecksumCRC32(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32(Aws::String&& value) { SetChecksumCRC32(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32(const char* value) { SetChecksumCRC32(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumCRC32C() const{ return m_checksumCRC32C; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool ChecksumCRC32CHasBeenSet() const { return m_checksumCRC32CHasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const Aws::String& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(Aws::String&& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const char* value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32C(const Aws::String& value) { SetChecksumCRC32C(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32C(Aws::String&& value) { SetChecksumCRC32C(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumCRC32C(const char* value) { SetChecksumCRC32C(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumSHA1() const{ return m_checksumSHA1; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool ChecksumSHA1HasBeenSet() const { return m_checksumSHA1HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const Aws::String& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(Aws::String&& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const char* value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA1(const Aws::String& value) { SetChecksumSHA1(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA1(Aws::String&& value) { SetChecksumSHA1(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA1(const char* value) { SetChecksumSHA1(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumSHA256() const{ return m_checksumSHA256; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool ChecksumSHA256HasBeenSet() const { return m_checksumSHA256HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const Aws::String& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(Aws::String&& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const char* value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA256(const Aws::String& value) { SetChecksumSHA256(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA256(Aws::String&& value) { SetChecksumSHA256(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This header specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithChecksumSHA256(const char* value) { SetChecksumSHA256(value); return *this;}


    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline const Aws::Utils::DateTime& GetExpires() const{ return m_expires; }

    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline bool ExpiresHasBeenSet() const { return m_expiresHasBeenSet; }

    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline void SetExpires(const Aws::Utils::DateTime& value) { m_expiresHasBeenSet = true; m_expires = value; }

    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline void SetExpires(Aws::Utils::DateTime&& value) { m_expiresHasBeenSet = true; m_expires = std::move(value); }

    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline PutObjectRequest& WithExpires(const Aws::Utils::DateTime& value) { SetExpires(value); return *this;}

    /**
     * <p>The date and time at which the object is no longer cacheable. For more
     * information, see <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21">http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21</a>.</p>
     */
    inline PutObjectRequest& WithExpires(Aws::Utils::DateTime&& value) { SetExpires(std::move(value)); return *this;}


    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline const Aws::String& GetGrantFullControl() const{ return m_grantFullControl; }

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline bool GrantFullControlHasBeenSet() const { return m_grantFullControlHasBeenSet; }

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantFullControl(const Aws::String& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = value; }

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantFullControl(Aws::String&& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = std::move(value); }

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantFullControl(const char* value) { m_grantFullControlHasBeenSet = true; m_grantFullControl.assign(value); }

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantFullControl(const Aws::String& value) { SetGrantFullControl(value); return *this;}

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantFullControl(Aws::String&& value) { SetGrantFullControl(std::move(value)); return *this;}

    /**
     * <p>Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the
     * object.</p> <p>This action is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantFullControl(const char* value) { SetGrantFullControl(value); return *this;}


    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline const Aws::String& GetGrantRead() const{ return m_grantRead; }

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline bool GrantReadHasBeenSet() const { return m_grantReadHasBeenSet; }

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantRead(const Aws::String& value) { m_grantReadHasBeenSet = true; m_grantRead = value; }

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantRead(Aws::String&& value) { m_grantReadHasBeenSet = true; m_grantRead = std::move(value); }

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantRead(const char* value) { m_grantReadHasBeenSet = true; m_grantRead.assign(value); }

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantRead(const Aws::String& value) { SetGrantRead(value); return *this;}

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantRead(Aws::String&& value) { SetGrantRead(std::move(value)); return *this;}

    /**
     * <p>Allows grantee to read the object data and its metadata.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantRead(const char* value) { SetGrantRead(value); return *this;}


    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline const Aws::String& GetGrantReadACP() const{ return m_grantReadACP; }

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline bool GrantReadACPHasBeenSet() const { return m_grantReadACPHasBeenSet; }

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline void SetGrantReadACP(const Aws::String& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = value; }

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline void SetGrantReadACP(Aws::String&& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = std::move(value); }

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline void SetGrantReadACP(const char* value) { m_grantReadACPHasBeenSet = true; m_grantReadACP.assign(value); }

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantReadACP(const Aws::String& value) { SetGrantReadACP(value); return *this;}

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantReadACP(Aws::String&& value) { SetGrantReadACP(std::move(value)); return *this;}

    /**
     * <p>Allows grantee to read the object ACL.</p> <p>This action is not supported by
     * Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantReadACP(const char* value) { SetGrantReadACP(value); return *this;}


    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline const Aws::String& GetGrantWriteACP() const{ return m_grantWriteACP; }

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline bool GrantWriteACPHasBeenSet() const { return m_grantWriteACPHasBeenSet; }

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantWriteACP(const Aws::String& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = value; }

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantWriteACP(Aws::String&& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = std::move(value); }

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline void SetGrantWriteACP(const char* value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP.assign(value); }

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantWriteACP(const Aws::String& value) { SetGrantWriteACP(value); return *this;}

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantWriteACP(Aws::String&& value) { SetGrantWriteACP(std::move(value)); return *this;}

    /**
     * <p>Allows grantee to write the ACL for the applicable object.</p> <p>This action
     * is not supported by Amazon S3 on Outposts.</p>
     */
    inline PutObjectRequest& WithGrantWriteACP(const char* value) { SetGrantWriteACP(value); return *this;}


    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline PutObjectRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline PutObjectRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>Object key for which the PUT action was initiated.</p>
     */
    inline PutObjectRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline const Aws::Map<Aws::String, Aws::String>& GetMetadata() const{ return m_metadata; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline bool MetadataHasBeenSet() const { return m_metadataHasBeenSet; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline void SetMetadata(const Aws::Map<Aws::String, Aws::String>& value) { m_metadataHasBeenSet = true; m_metadata = value; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline void SetMetadata(Aws::Map<Aws::String, Aws::String>&& value) { m_metadataHasBeenSet = true; m_metadata = std::move(value); }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& WithMetadata(const Aws::Map<Aws::String, Aws::String>& value) { SetMetadata(value); return *this;}

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& WithMetadata(Aws::Map<Aws::String, Aws::String>&& value) { SetMetadata(std::move(value)); return *this;}

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(const Aws::String& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(Aws::String&& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(const Aws::String& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(Aws::String&& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(const char* key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(Aws::String&& key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline PutObjectRequest& AddMetadata(const char* key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }


    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline bool ServerSideEncryptionHasBeenSet() const { return m_serverSideEncryptionHasBeenSet; }

    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = value; }

    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = std::move(value); }

    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline PutObjectRequest& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * <p>The server-side encryption algorithm used when storing this object in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline PutObjectRequest& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>By default, Amazon S3 uses the STANDARD Storage Class to store newly created
     * objects. The STANDARD storage class provides high durability and high
     * availability. Depending on performance needs, you can specify a different
     * Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline PutObjectRequest& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline const Aws::String& GetWebsiteRedirectLocation() const{ return m_websiteRedirectLocation; }

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline bool WebsiteRedirectLocationHasBeenSet() const { return m_websiteRedirectLocationHasBeenSet; }

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline void SetWebsiteRedirectLocation(const Aws::String& value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation = value; }

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline void SetWebsiteRedirectLocation(Aws::String&& value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation = std::move(value); }

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline void SetWebsiteRedirectLocation(const char* value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation.assign(value); }

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline PutObjectRequest& WithWebsiteRedirectLocation(const Aws::String& value) { SetWebsiteRedirectLocation(value); return *this;}

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline PutObjectRequest& WithWebsiteRedirectLocation(Aws::String&& value) { SetWebsiteRedirectLocation(std::move(value)); return *this;}

    /**
     * <p>If the bucket is configured as a website, redirects requests for this object
     * to another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata. For information about object
     * metadata, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">Object
     * Key and Metadata</a>.</p> <p>In the following example, the request header sets
     * the redirect to an object (anotherPage.html) in the same bucket:</p> <p>
     * <code>x-amz-website-redirect-location: /anotherPage.html</code> </p> <p>In the
     * following example, the request header sets the object redirect to another
     * website:</p> <p> <code>x-amz-website-redirect-location:
     * http://www.example.com/</code> </p> <p>For more information about website
     * hosting in Amazon S3, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html">Hosting
     * Websites on Amazon S3</a> and <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/how-to-page-redirect.html">How
     * to Configure Website Page Redirects</a>. </p>
     */
    inline PutObjectRequest& WithWebsiteRedirectLocation(const char* value) { SetWebsiteRedirectLocation(value); return *this;}


    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline bool SSECustomerAlgorithmHasBeenSet() const { return m_sSECustomerAlgorithmHasBeenSet; }

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = value; }

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = std::move(value); }

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm.assign(value); }

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline PutObjectRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline PutObjectRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * <p>Specifies the algorithm to use to when encrypting the object (for example,
     * AES256).</p>
     */
    inline PutObjectRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline const Aws::String& GetSSECustomerKey() const{ return m_sSECustomerKey; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline bool SSECustomerKeyHasBeenSet() const { return m_sSECustomerKeyHasBeenSet; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(const Aws::String& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = value; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(Aws::String&& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = std::move(value); }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(const char* value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey.assign(value); }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline PutObjectRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;}

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline PutObjectRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;}

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be
     * appropriate for use with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline PutObjectRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;}


    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline const Aws::String& GetSSECustomerKeyMD5() const{ return m_sSECustomerKeyMD5; }

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline bool SSECustomerKeyMD5HasBeenSet() const { return m_sSECustomerKeyMD5HasBeenSet; }

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline void SetSSECustomerKeyMD5(const Aws::String& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = value; }

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline void SetSSECustomerKeyMD5(Aws::String&& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = std::move(value); }

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline void SetSSECustomerKeyMD5(const char* value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5.assign(value); }

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline PutObjectRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline PutObjectRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline PutObjectRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline bool SSEKMSKeyIdHasBeenSet() const { return m_sSEKMSKeyIdHasBeenSet; }

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = value; }

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = std::move(value); }

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId.assign(value); }

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline PutObjectRequest& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline PutObjectRequest& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * <p>If <code>x-amz-server-side-encryption</code> is present and has the value of
     * <code>aws:kms</code>, this header specifies the ID of the Amazon Web Services
     * Key Management Service (Amazon Web Services KMS) symmetrical customer managed
     * key that was used for the object. If you specify
     * <code>x-amz-server-side-encryption:aws:kms</code>, but do not provide<code>
     * x-amz-server-side-encryption-aws-kms-key-id</code>, Amazon S3 uses the Amazon
     * Web Services managed key to protect the data. If the KMS key does not exist in
     * the same account issuing the command, you must use the full ARN and not just the
     * ID. </p>
     */
    inline PutObjectRequest& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline const Aws::String& GetSSEKMSEncryptionContext() const{ return m_sSEKMSEncryptionContext; }

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline bool SSEKMSEncryptionContextHasBeenSet() const { return m_sSEKMSEncryptionContextHasBeenSet; }

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline void SetSSEKMSEncryptionContext(const Aws::String& value) { m_sSEKMSEncryptionContextHasBeenSet = true; m_sSEKMSEncryptionContext = value; }

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline void SetSSEKMSEncryptionContext(Aws::String&& value) { m_sSEKMSEncryptionContextHasBeenSet = true; m_sSEKMSEncryptionContext = std::move(value); }

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline void SetSSEKMSEncryptionContext(const char* value) { m_sSEKMSEncryptionContextHasBeenSet = true; m_sSEKMSEncryptionContext.assign(value); }

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline PutObjectRequest& WithSSEKMSEncryptionContext(const Aws::String& value) { SetSSEKMSEncryptionContext(value); return *this;}

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline PutObjectRequest& WithSSEKMSEncryptionContext(Aws::String&& value) { SetSSEKMSEncryptionContext(std::move(value)); return *this;}

    /**
     * <p>Specifies the Amazon Web Services KMS Encryption Context to use for object
     * encryption. The value of this header is a base64-encoded UTF-8 string holding
     * JSON with the encryption context key-value pairs.</p>
     */
    inline PutObjectRequest& WithSSEKMSEncryptionContext(const char* value) { SetSSEKMSEncryptionContext(value); return *this;}


    /**
     * <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
     * with server-side encryption using AWS KMS (SSE-KMS). Setting this header to
     * <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption
     * with SSE-KMS.</p> <p>Specifying this header with a PUT action doesn’t affect
     * bucket-level settings for S3 Bucket Key.</p>
     */
    inline bool GetBucketKeyEnabled() const{ return m_bucketKeyEnabled; }

    /**
     * <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
     * with server-side encryption using AWS KMS (SSE-KMS). Setting this header to
     * <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption
     * with SSE-KMS.</p> <p>Specifying this header with a PUT action doesn’t affect
     * bucket-level settings for S3 Bucket Key.</p>
     */
    inline bool BucketKeyEnabledHasBeenSet() const { return m_bucketKeyEnabledHasBeenSet; }

    /**
     * <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
     * with server-side encryption using AWS KMS (SSE-KMS). Setting this header to
     * <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption
     * with SSE-KMS.</p> <p>Specifying this header with a PUT action doesn’t affect
     * bucket-level settings for S3 Bucket Key.</p>
     */
    inline void SetBucketKeyEnabled(bool value) { m_bucketKeyEnabledHasBeenSet = true; m_bucketKeyEnabled = value; }

    /**
     * <p>Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption
     * with server-side encryption using AWS KMS (SSE-KMS). Setting this header to
     * <code>true</code> causes Amazon S3 to use an S3 Bucket Key for object encryption
     * with SSE-KMS.</p> <p>Specifying this header with a PUT action doesn’t affect
     * bucket-level settings for S3 Bucket Key.</p>
     */
    inline PutObjectRequest& WithBucketKeyEnabled(bool value) { SetBucketKeyEnabled(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline bool RequestPayerHasBeenSet() const { return m_requestPayerHasBeenSet; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline PutObjectRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline PutObjectRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline const Aws::String& GetTagging() const{ return m_tagging; }

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline bool TaggingHasBeenSet() const { return m_taggingHasBeenSet; }

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline void SetTagging(const Aws::String& value) { m_taggingHasBeenSet = true; m_tagging = value; }

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline void SetTagging(Aws::String&& value) { m_taggingHasBeenSet = true; m_tagging = std::move(value); }

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline void SetTagging(const char* value) { m_taggingHasBeenSet = true; m_tagging.assign(value); }

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline PutObjectRequest& WithTagging(const Aws::String& value) { SetTagging(value); return *this;}

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline PutObjectRequest& WithTagging(Aws::String&& value) { SetTagging(std::move(value)); return *this;}

    /**
     * <p>The tag-set for the object. The tag-set must be encoded as URL Query
     * parameters. (For example, "Key1=Value1")</p>
     */
    inline PutObjectRequest& WithTagging(const char* value) { SetTagging(value); return *this;}


    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline const ObjectLockMode& GetObjectLockMode() const{ return m_objectLockMode; }

    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline bool ObjectLockModeHasBeenSet() const { return m_objectLockModeHasBeenSet; }

    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline void SetObjectLockMode(const ObjectLockMode& value) { m_objectLockModeHasBeenSet = true; m_objectLockMode = value; }

    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline void SetObjectLockMode(ObjectLockMode&& value) { m_objectLockModeHasBeenSet = true; m_objectLockMode = std::move(value); }

    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline PutObjectRequest& WithObjectLockMode(const ObjectLockMode& value) { SetObjectLockMode(value); return *this;}

    /**
     * <p>The Object Lock mode that you want to apply to this object.</p>
     */
    inline PutObjectRequest& WithObjectLockMode(ObjectLockMode&& value) { SetObjectLockMode(std::move(value)); return *this;}


    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline const Aws::Utils::DateTime& GetObjectLockRetainUntilDate() const{ return m_objectLockRetainUntilDate; }

    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline bool ObjectLockRetainUntilDateHasBeenSet() const { return m_objectLockRetainUntilDateHasBeenSet; }

    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline void SetObjectLockRetainUntilDate(const Aws::Utils::DateTime& value) { m_objectLockRetainUntilDateHasBeenSet = true; m_objectLockRetainUntilDate = value; }

    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline void SetObjectLockRetainUntilDate(Aws::Utils::DateTime&& value) { m_objectLockRetainUntilDateHasBeenSet = true; m_objectLockRetainUntilDate = std::move(value); }

    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline PutObjectRequest& WithObjectLockRetainUntilDate(const Aws::Utils::DateTime& value) { SetObjectLockRetainUntilDate(value); return *this;}

    /**
     * <p>The date and time when you want this object's Object Lock to expire. Must be
     * formatted as a timestamp parameter.</p>
     */
    inline PutObjectRequest& WithObjectLockRetainUntilDate(Aws::Utils::DateTime&& value) { SetObjectLockRetainUntilDate(std::move(value)); return *this;}


    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline const ObjectLockLegalHoldStatus& GetObjectLockLegalHoldStatus() const{ return m_objectLockLegalHoldStatus; }

    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline bool ObjectLockLegalHoldStatusHasBeenSet() const { return m_objectLockLegalHoldStatusHasBeenSet; }

    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline void SetObjectLockLegalHoldStatus(const ObjectLockLegalHoldStatus& value) { m_objectLockLegalHoldStatusHasBeenSet = true; m_objectLockLegalHoldStatus = value; }

    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline void SetObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus&& value) { m_objectLockLegalHoldStatusHasBeenSet = true; m_objectLockLegalHoldStatus = std::move(value); }

    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline PutObjectRequest& WithObjectLockLegalHoldStatus(const ObjectLockLegalHoldStatus& value) { SetObjectLockLegalHoldStatus(value); return *this;}

    /**
     * <p>Specifies whether a legal hold will be applied to this object. For more
     * information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline PutObjectRequest& WithObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus&& value) { SetObjectLockLegalHoldStatus(std::move(value)); return *this;}


    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline const Aws::String& GetExpectedBucketOwner() const{ return m_expectedBucketOwner; }

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline bool ExpectedBucketOwnerHasBeenSet() const { return m_expectedBucketOwnerHasBeenSet; }

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline void SetExpectedBucketOwner(const Aws::String& value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner = value; }

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline void SetExpectedBucketOwner(Aws::String&& value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner = std::move(value); }

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline void SetExpectedBucketOwner(const char* value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner.assign(value); }

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline PutObjectRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline PutObjectRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline PutObjectRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline PutObjectRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline PutObjectRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutObjectRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    ObjectCannedACL m_aCL;
    bool m_aCLHasBeenSet = false;


    Aws::String m_bucket;
    bool m_bucketHasBeenSet = false;

    Aws::String m_cacheControl;
    bool m_cacheControlHasBeenSet = false;

    Aws::String m_contentDisposition;
    bool m_contentDispositionHasBeenSet = false;

    Aws::String m_contentEncoding;
    bool m_contentEncodingHasBeenSet = false;

    Aws::String m_contentLanguage;
    bool m_contentLanguageHasBeenSet = false;

    long long m_contentLength;
    bool m_contentLengthHasBeenSet = false;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet = false;

    ChecksumAlgorithm m_checksumAlgorithm;
    bool m_checksumAlgorithmHasBeenSet = false;

    Aws::String m_checksumCRC32;
    bool m_checksumCRC32HasBeenSet = false;

    Aws::String m_checksumCRC32C;
    bool m_checksumCRC32CHasBeenSet = false;

    Aws::String m_checksumSHA1;
    bool m_checksumSHA1HasBeenSet = false;

    Aws::String m_checksumSHA256;
    bool m_checksumSHA256HasBeenSet = false;

    Aws::Utils::DateTime m_expires;
    bool m_expiresHasBeenSet = false;

    Aws::String m_grantFullControl;
    bool m_grantFullControlHasBeenSet = false;

    Aws::String m_grantRead;
    bool m_grantReadHasBeenSet = false;

    Aws::String m_grantReadACP;
    bool m_grantReadACPHasBeenSet = false;

    Aws::String m_grantWriteACP;
    bool m_grantWriteACPHasBeenSet = false;

    Aws::String m_key;
    bool m_keyHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_metadata;
    bool m_metadataHasBeenSet = false;

    ServerSideEncryption m_serverSideEncryption;
    bool m_serverSideEncryptionHasBeenSet = false;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet = false;

    Aws::String m_websiteRedirectLocation;
    bool m_websiteRedirectLocationHasBeenSet = false;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet = false;

    Aws::String m_sSECustomerKey;
    bool m_sSECustomerKeyHasBeenSet = false;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet = false;

    Aws::String m_sSEKMSKeyId;
    bool m_sSEKMSKeyIdHasBeenSet = false;

    Aws::String m_sSEKMSEncryptionContext;
    bool m_sSEKMSEncryptionContextHasBeenSet = false;

    bool m_bucketKeyEnabled;
    bool m_bucketKeyEnabledHasBeenSet = false;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet = false;

    Aws::String m_tagging;
    bool m_taggingHasBeenSet = false;

    ObjectLockMode m_objectLockMode;
    bool m_objectLockModeHasBeenSet = false;

    Aws::Utils::DateTime m_objectLockRetainUntilDate;
    bool m_objectLockRetainUntilDateHasBeenSet = false;

    ObjectLockLegalHoldStatus m_objectLockLegalHoldStatus;
    bool m_objectLockLegalHoldStatusHasBeenSet = false;

    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
