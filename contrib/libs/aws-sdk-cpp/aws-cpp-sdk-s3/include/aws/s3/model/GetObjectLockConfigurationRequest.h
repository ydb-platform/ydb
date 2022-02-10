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
  class AWS_S3_API GetObjectLockConfigurationRequest : public S3Request
  {
  public:
    GetObjectLockConfigurationRequest();
 
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetObjectLockConfiguration"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override; 

 
    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline GetObjectLockConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline GetObjectLockConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The bucket whose Object Lock configuration you want to retrieve.</p> <p>When 
     * using this API with an access point, you must direct requests to the access 
     * point hostname. The access point hostname takes the form 
     * <i>AccessPointName</i>-<i>AccountId</i>.s3-accesspoint.<i>Region</i>.amazonaws.com. 
     * When using this operation with an access point through the AWS SDKs, you provide 
     * the access point ARN in place of the bucket name. For more information about 
     * access point ARNs, see <a 
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/using-access-points.html">Using 
     * Access Points</a> in the <i>Amazon Simple Storage Service Developer 
     * Guide</i>.</p> 
     */
    inline GetObjectLockConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


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
    inline GetObjectLockConfigurationRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline GetObjectLockConfigurationRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline GetObjectLockConfigurationRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;} 
 
 
    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline GetObjectLockConfigurationRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline GetObjectLockConfigurationRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline GetObjectLockConfigurationRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_expectedBucketOwner; 
    bool m_expectedBucketOwnerHasBeenSet; 
 
    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
