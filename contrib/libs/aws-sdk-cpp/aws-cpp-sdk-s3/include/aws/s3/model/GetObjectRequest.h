/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/RequestPayer.h>
#include <aws/s3/model/ChecksumMode.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
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
  class GetObjectRequest : public S3Request
  {
  public:
    AWS_S3_API GetObjectRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetObject"; }

    AWS_S3_API Aws::String SerializePayload() const override;

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    AWS_S3_API bool ShouldValidateResponseChecksum() const override;

    AWS_S3_API Aws::Vector<Aws::String> GetResponseChecksumAlgorithmNames() const override;

    /**
     * Helper function to collect parameters (configurable and static hardcoded) required for endpoint computation.
     */
    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline GetObjectRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline GetObjectRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The bucket name containing the object. </p> <p>When using this action with an
     * access point, you must direct requests to the access point hostname. The access
     * point hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com.
     * When using this action with an access point through the Amazon Web Services
     * SDKs, you provide the access point ARN in place of the bucket name. For more
     * information about access point ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Using
     * access points</a> in the <i>Amazon S3 User Guide</i>.</p> <p>When using an
     * Object Lambda access point the hostname takes the form
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-object-lambda.<i>Region</i>.amazonaws.com.</p>
     * <p>When using this action with Amazon S3 on Outposts, you must direct requests
     * to the S3 on Outposts hostname. The S3 on Outposts hostname takes the form
     * <code>
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com</code>.
     * When using this action with S3 on Outposts through the Amazon Web Services SDKs,
     * you provide the Outposts bucket ARN in place of the bucket name. For more
     * information about S3 on Outposts ARNs, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html">Using
     * Amazon S3 on Outposts</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline GetObjectRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline const Aws::String& GetIfMatch() const{ return m_ifMatch; }

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline bool IfMatchHasBeenSet() const { return m_ifMatchHasBeenSet; }

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline void SetIfMatch(const Aws::String& value) { m_ifMatchHasBeenSet = true; m_ifMatch = value; }

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline void SetIfMatch(Aws::String&& value) { m_ifMatchHasBeenSet = true; m_ifMatch = std::move(value); }

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline void SetIfMatch(const char* value) { m_ifMatchHasBeenSet = true; m_ifMatch.assign(value); }

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline GetObjectRequest& WithIfMatch(const Aws::String& value) { SetIfMatch(value); return *this;}

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline GetObjectRequest& WithIfMatch(Aws::String&& value) { SetIfMatch(std::move(value)); return *this;}

    /**
     * <p>Return the object only if its entity tag (ETag) is the same as the one
     * specified; otherwise, return a 412 (precondition failed) error.</p>
     */
    inline GetObjectRequest& WithIfMatch(const char* value) { SetIfMatch(value); return *this;}


    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline const Aws::Utils::DateTime& GetIfModifiedSince() const{ return m_ifModifiedSince; }

    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline bool IfModifiedSinceHasBeenSet() const { return m_ifModifiedSinceHasBeenSet; }

    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline void SetIfModifiedSince(const Aws::Utils::DateTime& value) { m_ifModifiedSinceHasBeenSet = true; m_ifModifiedSince = value; }

    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline void SetIfModifiedSince(Aws::Utils::DateTime&& value) { m_ifModifiedSinceHasBeenSet = true; m_ifModifiedSince = std::move(value); }

    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline GetObjectRequest& WithIfModifiedSince(const Aws::Utils::DateTime& value) { SetIfModifiedSince(value); return *this;}

    /**
     * <p>Return the object only if it has been modified since the specified time;
     * otherwise, return a 304 (not modified) error.</p>
     */
    inline GetObjectRequest& WithIfModifiedSince(Aws::Utils::DateTime&& value) { SetIfModifiedSince(std::move(value)); return *this;}


    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline const Aws::String& GetIfNoneMatch() const{ return m_ifNoneMatch; }

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline bool IfNoneMatchHasBeenSet() const { return m_ifNoneMatchHasBeenSet; }

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline void SetIfNoneMatch(const Aws::String& value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch = value; }

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline void SetIfNoneMatch(Aws::String&& value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch = std::move(value); }

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline void SetIfNoneMatch(const char* value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch.assign(value); }

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline GetObjectRequest& WithIfNoneMatch(const Aws::String& value) { SetIfNoneMatch(value); return *this;}

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline GetObjectRequest& WithIfNoneMatch(Aws::String&& value) { SetIfNoneMatch(std::move(value)); return *this;}

    /**
     * <p>Return the object only if its entity tag (ETag) is different from the one
     * specified; otherwise, return a 304 (not modified) error.</p>
     */
    inline GetObjectRequest& WithIfNoneMatch(const char* value) { SetIfNoneMatch(value); return *this;}


    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline const Aws::Utils::DateTime& GetIfUnmodifiedSince() const{ return m_ifUnmodifiedSince; }

    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline bool IfUnmodifiedSinceHasBeenSet() const { return m_ifUnmodifiedSinceHasBeenSet; }

    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline void SetIfUnmodifiedSince(const Aws::Utils::DateTime& value) { m_ifUnmodifiedSinceHasBeenSet = true; m_ifUnmodifiedSince = value; }

    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline void SetIfUnmodifiedSince(Aws::Utils::DateTime&& value) { m_ifUnmodifiedSinceHasBeenSet = true; m_ifUnmodifiedSince = std::move(value); }

    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline GetObjectRequest& WithIfUnmodifiedSince(const Aws::Utils::DateTime& value) { SetIfUnmodifiedSince(value); return *this;}

    /**
     * <p>Return the object only if it has not been modified since the specified time;
     * otherwise, return a 412 (precondition failed) error.</p>
     */
    inline GetObjectRequest& WithIfUnmodifiedSince(Aws::Utils::DateTime&& value) { SetIfUnmodifiedSince(std::move(value)); return *this;}


    /**
     * <p>Key of the object to get.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>Key of the object to get.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Key of the object to get.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>Key of the object to get.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>Key of the object to get.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>Key of the object to get.</p>
     */
    inline GetObjectRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>Key of the object to get.</p>
     */
    inline GetObjectRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>Key of the object to get.</p>
     */
    inline GetObjectRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline const Aws::String& GetRange() const{ return m_range; }

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline bool RangeHasBeenSet() const { return m_rangeHasBeenSet; }

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline void SetRange(const Aws::String& value) { m_rangeHasBeenSet = true; m_range = value; }

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline void SetRange(Aws::String&& value) { m_rangeHasBeenSet = true; m_range = std::move(value); }

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline void SetRange(const char* value) { m_rangeHasBeenSet = true; m_range.assign(value); }

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline GetObjectRequest& WithRange(const Aws::String& value) { SetRange(value); return *this;}

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline GetObjectRequest& WithRange(Aws::String&& value) { SetRange(std::move(value)); return *this;}

    /**
     * <p>Downloads the specified range bytes of an object. For more information about
     * the HTTP Range header, see <a
     * href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35</a>.</p>
     *  <p>Amazon S3 doesn't support retrieving multiple ranges of data per
     * <code>GET</code> request.</p> 
     */
    inline GetObjectRequest& WithRange(const char* value) { SetRange(value); return *this;}


    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline const Aws::String& GetResponseCacheControl() const{ return m_responseCacheControl; }

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline bool ResponseCacheControlHasBeenSet() const { return m_responseCacheControlHasBeenSet; }

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline void SetResponseCacheControl(const Aws::String& value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl = value; }

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline void SetResponseCacheControl(Aws::String&& value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl = std::move(value); }

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline void SetResponseCacheControl(const char* value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl.assign(value); }

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseCacheControl(const Aws::String& value) { SetResponseCacheControl(value); return *this;}

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseCacheControl(Aws::String&& value) { SetResponseCacheControl(std::move(value)); return *this;}

    /**
     * <p>Sets the <code>Cache-Control</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseCacheControl(const char* value) { SetResponseCacheControl(value); return *this;}


    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline const Aws::String& GetResponseContentDisposition() const{ return m_responseContentDisposition; }

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline bool ResponseContentDispositionHasBeenSet() const { return m_responseContentDispositionHasBeenSet; }

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline void SetResponseContentDisposition(const Aws::String& value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition = value; }

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline void SetResponseContentDisposition(Aws::String&& value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition = std::move(value); }

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline void SetResponseContentDisposition(const char* value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition.assign(value); }

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline GetObjectRequest& WithResponseContentDisposition(const Aws::String& value) { SetResponseContentDisposition(value); return *this;}

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline GetObjectRequest& WithResponseContentDisposition(Aws::String&& value) { SetResponseContentDisposition(std::move(value)); return *this;}

    /**
     * <p>Sets the <code>Content-Disposition</code> header of the response</p>
     */
    inline GetObjectRequest& WithResponseContentDisposition(const char* value) { SetResponseContentDisposition(value); return *this;}


    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline const Aws::String& GetResponseContentEncoding() const{ return m_responseContentEncoding; }

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline bool ResponseContentEncodingHasBeenSet() const { return m_responseContentEncodingHasBeenSet; }

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline void SetResponseContentEncoding(const Aws::String& value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding = value; }

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline void SetResponseContentEncoding(Aws::String&& value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding = std::move(value); }

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline void SetResponseContentEncoding(const char* value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding.assign(value); }

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentEncoding(const Aws::String& value) { SetResponseContentEncoding(value); return *this;}

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentEncoding(Aws::String&& value) { SetResponseContentEncoding(std::move(value)); return *this;}

    /**
     * <p>Sets the <code>Content-Encoding</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentEncoding(const char* value) { SetResponseContentEncoding(value); return *this;}


    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline const Aws::String& GetResponseContentLanguage() const{ return m_responseContentLanguage; }

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline bool ResponseContentLanguageHasBeenSet() const { return m_responseContentLanguageHasBeenSet; }

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline void SetResponseContentLanguage(const Aws::String& value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage = value; }

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline void SetResponseContentLanguage(Aws::String&& value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage = std::move(value); }

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline void SetResponseContentLanguage(const char* value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage.assign(value); }

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentLanguage(const Aws::String& value) { SetResponseContentLanguage(value); return *this;}

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentLanguage(Aws::String&& value) { SetResponseContentLanguage(std::move(value)); return *this;}

    /**
     * <p>Sets the <code>Content-Language</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentLanguage(const char* value) { SetResponseContentLanguage(value); return *this;}


    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline const Aws::String& GetResponseContentType() const{ return m_responseContentType; }

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline bool ResponseContentTypeHasBeenSet() const { return m_responseContentTypeHasBeenSet; }

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline void SetResponseContentType(const Aws::String& value) { m_responseContentTypeHasBeenSet = true; m_responseContentType = value; }

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline void SetResponseContentType(Aws::String&& value) { m_responseContentTypeHasBeenSet = true; m_responseContentType = std::move(value); }

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline void SetResponseContentType(const char* value) { m_responseContentTypeHasBeenSet = true; m_responseContentType.assign(value); }

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentType(const Aws::String& value) { SetResponseContentType(value); return *this;}

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentType(Aws::String&& value) { SetResponseContentType(std::move(value)); return *this;}

    /**
     * <p>Sets the <code>Content-Type</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseContentType(const char* value) { SetResponseContentType(value); return *this;}


    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline const Aws::Utils::DateTime& GetResponseExpires() const{ return m_responseExpires; }

    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline bool ResponseExpiresHasBeenSet() const { return m_responseExpiresHasBeenSet; }

    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline void SetResponseExpires(const Aws::Utils::DateTime& value) { m_responseExpiresHasBeenSet = true; m_responseExpires = value; }

    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline void SetResponseExpires(Aws::Utils::DateTime&& value) { m_responseExpiresHasBeenSet = true; m_responseExpires = std::move(value); }

    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseExpires(const Aws::Utils::DateTime& value) { SetResponseExpires(value); return *this;}

    /**
     * <p>Sets the <code>Expires</code> header of the response.</p>
     */
    inline GetObjectRequest& WithResponseExpires(Aws::Utils::DateTime&& value) { SetResponseExpires(std::move(value)); return *this;}


    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline GetObjectRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline GetObjectRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>VersionId used to reference a specific version of the object.</p>
     */
    inline GetObjectRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline bool SSECustomerAlgorithmHasBeenSet() const { return m_sSECustomerAlgorithmHasBeenSet; }

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = value; }

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = std::move(value); }

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm.assign(value); }

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline GetObjectRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline GetObjectRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * <p>Specifies the algorithm to use to when decrypting the object (for example,
     * AES256).</p>
     */
    inline GetObjectRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline const Aws::String& GetSSECustomerKey() const{ return m_sSECustomerKey; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline bool SSECustomerKeyHasBeenSet() const { return m_sSECustomerKeyHasBeenSet; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(const Aws::String& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = value; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(Aws::String&& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = std::move(value); }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline void SetSSECustomerKey(const char* value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey.assign(value); }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline GetObjectRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;}

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline GetObjectRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;}

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 used to encrypt
     * the data. This value is used to decrypt the object when recovering it and must
     * match the one used when storing the data. The key must be appropriate for use
     * with the algorithm specified in the
     * <code>x-amz-server-side-encryption-customer-algorithm</code> header.</p>
     */
    inline GetObjectRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;}


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
    inline GetObjectRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline GetObjectRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure that the
     * encryption key was transmitted without error.</p>
     */
    inline GetObjectRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline bool RequestPayerHasBeenSet() const { return m_requestPayerHasBeenSet; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline GetObjectRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline GetObjectRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


    /**
     * <p>Part number of the object being read. This is a positive integer between 1
     * and 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.</p>
     */
    inline int GetPartNumber() const{ return m_partNumber; }

    /**
     * <p>Part number of the object being read. This is a positive integer between 1
     * and 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.</p>
     */
    inline bool PartNumberHasBeenSet() const { return m_partNumberHasBeenSet; }

    /**
     * <p>Part number of the object being read. This is a positive integer between 1
     * and 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.</p>
     */
    inline void SetPartNumber(int value) { m_partNumberHasBeenSet = true; m_partNumber = value; }

    /**
     * <p>Part number of the object being read. This is a positive integer between 1
     * and 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.</p>
     */
    inline GetObjectRequest& WithPartNumber(int value) { SetPartNumber(value); return *this;}


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
    inline GetObjectRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline GetObjectRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline GetObjectRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline const ChecksumMode& GetChecksumMode() const{ return m_checksumMode; }

    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline bool ChecksumModeHasBeenSet() const { return m_checksumModeHasBeenSet; }

    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline void SetChecksumMode(const ChecksumMode& value) { m_checksumModeHasBeenSet = true; m_checksumMode = value; }

    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline void SetChecksumMode(ChecksumMode&& value) { m_checksumModeHasBeenSet = true; m_checksumMode = std::move(value); }

    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline GetObjectRequest& WithChecksumMode(const ChecksumMode& value) { SetChecksumMode(value); return *this;}

    /**
     * <p>To retrieve the checksum, this mode must be enabled.</p>
     */
    inline GetObjectRequest& WithChecksumMode(ChecksumMode&& value) { SetChecksumMode(std::move(value)); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline GetObjectRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline GetObjectRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline GetObjectRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet = false;

    Aws::String m_ifMatch;
    bool m_ifMatchHasBeenSet = false;

    Aws::Utils::DateTime m_ifModifiedSince;
    bool m_ifModifiedSinceHasBeenSet = false;

    Aws::String m_ifNoneMatch;
    bool m_ifNoneMatchHasBeenSet = false;

    Aws::Utils::DateTime m_ifUnmodifiedSince;
    bool m_ifUnmodifiedSinceHasBeenSet = false;

    Aws::String m_key;
    bool m_keyHasBeenSet = false;

    Aws::String m_range;
    bool m_rangeHasBeenSet = false;

    Aws::String m_responseCacheControl;
    bool m_responseCacheControlHasBeenSet = false;

    Aws::String m_responseContentDisposition;
    bool m_responseContentDispositionHasBeenSet = false;

    Aws::String m_responseContentEncoding;
    bool m_responseContentEncodingHasBeenSet = false;

    Aws::String m_responseContentLanguage;
    bool m_responseContentLanguageHasBeenSet = false;

    Aws::String m_responseContentType;
    bool m_responseContentTypeHasBeenSet = false;

    Aws::Utils::DateTime m_responseExpires;
    bool m_responseExpiresHasBeenSet = false;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet = false;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet = false;

    Aws::String m_sSECustomerKey;
    bool m_sSECustomerKeyHasBeenSet = false;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet = false;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet = false;

    int m_partNumber;
    bool m_partNumberHasBeenSet = false;

    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet = false;

    ChecksumMode m_checksumMode;
    bool m_checksumModeHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
