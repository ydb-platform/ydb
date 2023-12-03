/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/ChecksumAlgorithm.h>
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
  class PutBucketPolicyRequest : public StreamingS3Request
  {
  public:
    AWS_S3_API PutBucketPolicyRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketPolicy"; }

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    AWS_S3_API Aws::String GetChecksumAlgorithmName() const override;

    /**
     * Helper function to collect parameters (configurable and static hardcoded) required for endpoint computation.
     */
    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    /**
     * <p>The name of the bucket.</p>
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline bool BucketHasBeenSet() const { return m_bucketHasBeenSet; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * <p>The name of the bucket.</p>
     */
    inline PutBucketPolicyRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The name of the bucket.</p>
     */
    inline PutBucketPolicyRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The name of the bucket.</p>
     */
    inline PutBucketPolicyRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline bool ContentMD5HasBeenSet() const { return m_contentMD5HasBeenSet; }

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline PutBucketPolicyRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline PutBucketPolicyRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    /**
     * <p>The MD5 hash of the request body.</p> <p>For requests made using the Amazon
     * Web Services Command Line Interface (CLI) or Amazon Web Services SDKs, this
     * field is calculated automatically.</p>
     */
    inline PutBucketPolicyRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


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
    inline PutBucketPolicyRequest& WithChecksumAlgorithm(const ChecksumAlgorithm& value) { SetChecksumAlgorithm(value); return *this;}

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
    inline PutBucketPolicyRequest& WithChecksumAlgorithm(ChecksumAlgorithm&& value) { SetChecksumAlgorithm(std::move(value)); return *this;}


    /**
     * <p>Set this parameter to true to confirm that you want to remove your
     * permissions to change this bucket policy in the future.</p>
     */
    inline bool GetConfirmRemoveSelfBucketAccess() const{ return m_confirmRemoveSelfBucketAccess; }

    /**
     * <p>Set this parameter to true to confirm that you want to remove your
     * permissions to change this bucket policy in the future.</p>
     */
    inline bool ConfirmRemoveSelfBucketAccessHasBeenSet() const { return m_confirmRemoveSelfBucketAccessHasBeenSet; }

    /**
     * <p>Set this parameter to true to confirm that you want to remove your
     * permissions to change this bucket policy in the future.</p>
     */
    inline void SetConfirmRemoveSelfBucketAccess(bool value) { m_confirmRemoveSelfBucketAccessHasBeenSet = true; m_confirmRemoveSelfBucketAccess = value; }

    /**
     * <p>Set this parameter to true to confirm that you want to remove your
     * permissions to change this bucket policy in the future.</p>
     */
    inline PutBucketPolicyRequest& WithConfirmRemoveSelfBucketAccess(bool value) { SetConfirmRemoveSelfBucketAccess(value); return *this;}


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
    inline PutBucketPolicyRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline PutBucketPolicyRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;}

    /**
     * <p>The account ID of the expected bucket owner. If the bucket is owned by a
     * different account, the request fails with the HTTP status code <code>403
     * Forbidden</code> (access denied).</p>
     */
    inline PutBucketPolicyRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline PutBucketPolicyRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline PutBucketPolicyRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline PutBucketPolicyRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet = false;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet = false;

    ChecksumAlgorithm m_checksumAlgorithm;
    bool m_checksumAlgorithmHasBeenSet = false;

    bool m_confirmRemoveSelfBucketAccess;
    bool m_confirmRemoveSelfBucketAccessHasBeenSet = false;


    Aws::String m_expectedBucketOwner;
    bool m_expectedBucketOwnerHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
