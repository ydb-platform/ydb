/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/EncodingType.h>
#include <aws/s3/model/RequestPayer.h>
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
  class ListObjectsRequest : public S3Request
  {
  public:
    AWS_S3_API ListObjectsRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListObjects"; }

    AWS_S3_API Aws::String SerializePayload() const override;

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    /**
     * Helper function to collect parameters (configurable and static hardcoded) required for endpoint computation.
     */
    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    /**
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
    inline ListObjectsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
    inline ListObjectsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The name of the bucket containing the objects.</p> <p>When using this action
     * with an access point, you must direct requests to the access point hostname. The
     * access point hostname takes the form
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
    inline ListObjectsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline const Aws::String& GetDelimiter() const{ return m_delimiter; }

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline bool DelimiterHasBeenSet() const { return m_delimiterHasBeenSet; }

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline void SetDelimiter(const Aws::String& value) { m_delimiterHasBeenSet = true; m_delimiter = value; }

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline void SetDelimiter(Aws::String&& value) { m_delimiterHasBeenSet = true; m_delimiter = std::move(value); }

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline void SetDelimiter(const char* value) { m_delimiterHasBeenSet = true; m_delimiter.assign(value); }

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline ListObjectsRequest& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline ListObjectsRequest& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    /**
     * <p>A delimiter is a character you use to group keys.</p>
     */
    inline ListObjectsRequest& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    
    inline const EncodingType& GetEncodingType() const{ return m_encodingType; }

    
    inline bool EncodingTypeHasBeenSet() const { return m_encodingTypeHasBeenSet; }

    
    inline void SetEncodingType(const EncodingType& value) { m_encodingTypeHasBeenSet = true; m_encodingType = value; }

    
    inline void SetEncodingType(EncodingType&& value) { m_encodingTypeHasBeenSet = true; m_encodingType = std::move(value); }

    
    inline ListObjectsRequest& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    
    inline ListObjectsRequest& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}


    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline const Aws::String& GetMarker() const{ return m_marker; }

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline bool MarkerHasBeenSet() const { return m_markerHasBeenSet; }

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline void SetMarker(const Aws::String& value) { m_markerHasBeenSet = true; m_marker = value; }

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline void SetMarker(Aws::String&& value) { m_markerHasBeenSet = true; m_marker = std::move(value); }

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline void SetMarker(const char* value) { m_markerHasBeenSet = true; m_marker.assign(value); }

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline ListObjectsRequest& WithMarker(const Aws::String& value) { SetMarker(value); return *this;}

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline ListObjectsRequest& WithMarker(Aws::String&& value) { SetMarker(std::move(value)); return *this;}

    /**
     * <p>Marker is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. Marker can be any key in the bucket.</p>
     */
    inline ListObjectsRequest& WithMarker(const char* value) { SetMarker(value); return *this;}


    /**
     * <p>Sets the maximum number of keys returned in the response. By default the
     * action returns up to 1,000 key names. The response might contain fewer keys but
     * will never contain more. </p>
     */
    inline int GetMaxKeys() const{ return m_maxKeys; }

    /**
     * <p>Sets the maximum number of keys returned in the response. By default the
     * action returns up to 1,000 key names. The response might contain fewer keys but
     * will never contain more. </p>
     */
    inline bool MaxKeysHasBeenSet() const { return m_maxKeysHasBeenSet; }

    /**
     * <p>Sets the maximum number of keys returned in the response. By default the
     * action returns up to 1,000 key names. The response might contain fewer keys but
     * will never contain more. </p>
     */
    inline void SetMaxKeys(int value) { m_maxKeysHasBeenSet = true; m_maxKeys = value; }

    /**
     * <p>Sets the maximum number of keys returned in the response. By default the
     * action returns up to 1,000 key names. The response might contain fewer keys but
     * will never contain more. </p>
     */
    inline ListObjectsRequest& WithMaxKeys(int value) { SetMaxKeys(value); return *this;}


    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline bool PrefixHasBeenSet() const { return m_prefixHasBeenSet; }

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline ListObjectsRequest& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline ListObjectsRequest& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * <p>Limits the response to keys that begin with the specified prefix.</p>
     */
    inline ListObjectsRequest& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline bool RequestPayerHasBeenSet() const { return m_requestPayerHasBeenSet; }

    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline ListObjectsRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    /**
     * <p>Confirms that the requester knows that she or he will be charged for the list
     * objects request. Bucket owners need not specify this parameter in their
     * requests.</p>
     */
    inline ListObjectsRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


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
    inline ListObjectsRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline ListObjectsRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline ListObjectsRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline ListObjectsRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline ListObjectsRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline ListObjectsRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet = false;

    Aws::String m_delimiter;
    bool m_delimiterHasBeenSet = false;

    EncodingType m_encodingType;
    bool m_encodingTypeHasBeenSet = false;

    Aws::String m_marker;
    bool m_markerHasBeenSet = false;

    int m_maxKeys;
    bool m_maxKeysHasBeenSet = false;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet = false;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet = false;

    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
