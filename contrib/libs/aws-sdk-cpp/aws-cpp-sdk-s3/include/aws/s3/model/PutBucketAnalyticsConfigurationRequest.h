/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/AnalyticsConfiguration.h>
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
  class AWS_S3_API PutBucketAnalyticsConfigurationRequest : public S3Request
  {
  public:
    PutBucketAnalyticsConfigurationRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketAnalyticsConfiguration"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; } 
 
    /** 
     * <p>The name of the bucket to which an analytics configuration is stored.</p> 
     */ 
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The name of the bucket to which an analytics configuration is stored.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline bool IdHasBeenSet() const { return m_idHasBeenSet; } 
 
    /** 
     * <p>The ID that identifies the analytics configuration.</p>
     */ 
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * <p>The ID that identifies the analytics configuration.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(const char* value) { SetId(value); return *this;}


    /**
     * <p>The configuration and any analyses for the analytics filter.</p>
     */
    inline const AnalyticsConfiguration& GetAnalyticsConfiguration() const{ return m_analyticsConfiguration; }

    /**
     * <p>The configuration and any analyses for the analytics filter.</p>
     */
    inline bool AnalyticsConfigurationHasBeenSet() const { return m_analyticsConfigurationHasBeenSet; } 
 
    /** 
     * <p>The configuration and any analyses for the analytics filter.</p> 
     */ 
    inline void SetAnalyticsConfiguration(const AnalyticsConfiguration& value) { m_analyticsConfigurationHasBeenSet = true; m_analyticsConfiguration = value; }

    /**
     * <p>The configuration and any analyses for the analytics filter.</p>
     */
    inline void SetAnalyticsConfiguration(AnalyticsConfiguration&& value) { m_analyticsConfigurationHasBeenSet = true; m_analyticsConfiguration = std::move(value); }

    /**
     * <p>The configuration and any analyses for the analytics filter.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithAnalyticsConfiguration(const AnalyticsConfiguration& value) { SetAnalyticsConfiguration(value); return *this;}

    /**
     * <p>The configuration and any analyses for the analytics filter.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithAnalyticsConfiguration(AnalyticsConfiguration&& value) { SetAnalyticsConfiguration(std::move(value)); return *this;}


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
    inline PutBucketAnalyticsConfigurationRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account id of the expected bucket owner. If the bucket is owned by a
     * different account, the request will fail with an HTTP <code>403 (Access
     * Denied)</code> error.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account id of the expected bucket owner. If the bucket is owned by a
     * different account, the request will fail with an HTTP <code>403 (Access
     * Denied)</code> error.</p>
     */
    inline PutBucketAnalyticsConfigurationRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; } 
 
     
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline PutBucketAnalyticsConfigurationRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline PutBucketAnalyticsConfigurationRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketAnalyticsConfigurationRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_id;
    bool m_idHasBeenSet;

    AnalyticsConfiguration m_analyticsConfiguration;
    bool m_analyticsConfigurationHasBeenSet;

    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
