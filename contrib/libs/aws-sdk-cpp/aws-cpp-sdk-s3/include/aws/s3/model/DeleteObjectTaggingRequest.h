/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 
 
#pragma once 
#include <aws/s3/S3_EXPORTS.h> 
#include <aws/s3/S3Request.h> 
#include <aws/core/utils/memory/stl/AWSString.h> 
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
  class AWS_S3_API DeleteObjectTaggingRequest : public S3Request 
  { 
  public: 
    DeleteObjectTaggingRequest(); 
 
    // Service request name is the Operation name which will send this request out, 
    // each operation should has unique request name, so that we can get operation's name from this request. 
    // Note: this is not true for response, multiple operations may have the same response name, 
    // so we can not get operation's name from response. 
    inline virtual const char* GetServiceRequestName() const override { return "DeleteObjectTagging"; } 
 
    Aws::String SerializePayload() const override; 
 
    void AddQueryStringParameters(Aws::Http::URI& uri) const override; 
 
    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override; 
 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; } 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; } 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); } 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); } 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline DeleteObjectTaggingRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;} 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline DeleteObjectTaggingRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;} 
 
    /**
     * <p>The bucket name containing the objects from which to remove the tags. </p> 
     * <p>When using this API with an access point, you must direct requests to the 
     * access point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> <p>When using this API with Amazon S3 on Outposts, you must 
     * direct requests to the S3 on Outposts hostname. The S3 on Outposts hostname 
     * takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.<i>outpostID</i>.s3-outposts.<i>Region</i>.amazonaws.com. 
     * When using this operation using S3 on Outposts through the AWS SDKs, you provide 
     * the Outposts bucket ARN in place of the bucket name. For more information about 
     * S3 on Outposts ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/S3onOutposts.html">Using 
     * S3 on Outposts</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline DeleteObjectTaggingRequest& WithBucket(const char* value) { SetBucket(value); return *this;} 
 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline const Aws::String& GetKey() const{ return m_key; } 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Name of the object key.</p> 
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; } 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); } 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); } 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline DeleteObjectTaggingRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;} 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline DeleteObjectTaggingRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;} 
 
    /**
     * <p>Name of the object key.</p> 
     */
    inline DeleteObjectTaggingRequest& WithKey(const char* value) { SetKey(value); return *this;} 
 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline const Aws::String& GetVersionId() const{ return m_versionId; } 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>The versionId of the object that the tag-set will be removed from.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; } 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); } 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); } 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;} 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;} 
 
    /** 
     * <p>The versionId of the object that the tag-set will be removed from.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;} 
 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline const Aws::String& GetExpectedBucketOwner() const{ return m_expectedBucketOwner; } 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline bool ExpectedBucketOwnerHasBeenSet() const { return m_expectedBucketOwnerHasBeenSet; } 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline void SetExpectedBucketOwner(const Aws::String& value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner = value; } 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline void SetExpectedBucketOwner(Aws::String&& value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner = std::move(value); } 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline void SetExpectedBucketOwner(const char* value) { m_expectedBucketOwnerHasBeenSet = true; m_expectedBucketOwner.assign(value); } 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline DeleteObjectTaggingRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;} 
 
 
     
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; } 
 
     
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; } 
 
     
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); } 
 
     
    inline DeleteObjectTaggingRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;} 
 
     
    inline DeleteObjectTaggingRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;} 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; } 
 
     
    inline DeleteObjectTaggingRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; } 
 
  private: 
 
    Aws::String m_bucket; 
    bool m_bucketHasBeenSet; 
 
    Aws::String m_key; 
    bool m_keyHasBeenSet; 
 
    Aws::String m_versionId; 
    bool m_versionIdHasBeenSet; 
 
    Aws::String m_expectedBucketOwner; 
    bool m_expectedBucketOwnerHasBeenSet; 
 
    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag; 
    bool m_customizedAccessLogTagHasBeenSet; 
  }; 
 
} // namespace Model 
} // namespace S3 
} // namespace Aws 
