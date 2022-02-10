/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/BucketLifecycleConfiguration.h>
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
  class AWS_S3_API PutBucketLifecycleConfigurationRequest : public S3Request
  {
  public:
    PutBucketLifecycleConfigurationRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketLifecycleConfiguration"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    inline bool ShouldComputeContentMd5() const override { return true; }


    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; } 
 
    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline PutBucketLifecycleConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline PutBucketLifecycleConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /** 
     * <p>The name of the bucket for which to set the configuration.</p>
     */ 
    inline PutBucketLifecycleConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline const BucketLifecycleConfiguration& GetLifecycleConfiguration() const{ return m_lifecycleConfiguration; }

    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline bool LifecycleConfigurationHasBeenSet() const { return m_lifecycleConfigurationHasBeenSet; } 
 
    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline void SetLifecycleConfiguration(const BucketLifecycleConfiguration& value) { m_lifecycleConfigurationHasBeenSet = true; m_lifecycleConfiguration = value; }

    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline void SetLifecycleConfiguration(BucketLifecycleConfiguration&& value) { m_lifecycleConfigurationHasBeenSet = true; m_lifecycleConfiguration = std::move(value); }

    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline PutBucketLifecycleConfigurationRequest& WithLifecycleConfiguration(const BucketLifecycleConfiguration& value) { SetLifecycleConfiguration(value); return *this;}

    /** 
     * <p>Container for lifecycle rules. You can add as many as 1,000 rules.</p>
     */ 
    inline PutBucketLifecycleConfigurationRequest& WithLifecycleConfiguration(BucketLifecycleConfiguration&& value) { SetLifecycleConfiguration(std::move(value)); return *this;}


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
    inline PutBucketLifecycleConfigurationRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account id of the expected bucket owner. If the bucket is owned by a
     * different account, the request will fail with an HTTP <code>403 (Access
     * Denied)</code> error.</p>
     */
    inline PutBucketLifecycleConfigurationRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account id of the expected bucket owner. If the bucket is owned by a
     * different account, the request will fail with an HTTP <code>403 (Access
     * Denied)</code> error.</p>
     */
    inline PutBucketLifecycleConfigurationRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; } 
 
     
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline PutBucketLifecycleConfigurationRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline PutBucketLifecycleConfigurationRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketLifecycleConfigurationRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    BucketLifecycleConfiguration m_lifecycleConfiguration;
    bool m_lifecycleConfigurationHasBeenSet;

    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
