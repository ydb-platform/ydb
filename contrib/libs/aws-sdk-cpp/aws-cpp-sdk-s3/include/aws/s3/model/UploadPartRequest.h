/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 
 
#pragma once 
#include <aws/s3/S3_EXPORTS.h> 
#include <aws/s3/S3Request.h> 
#include <aws/core/utils/Array.h> 
#include <aws/core/utils/memory/stl/AWSString.h> 
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
  class AWS_S3_API UploadPartRequest : public StreamingS3Request 
  { 
  public: 
    UploadPartRequest(); 
 
    // Service request name is the Operation name which will send this request out, 
    // each operation should has unique request name, so that we can get operation's name from this request. 
    // Note: this is not true for response, multiple operations may have the same response name, 
    // so we can not get operation's name from response. 
    inline virtual const char* GetServiceRequestName() const override { return "UploadPart"; } 
 
    void AddQueryStringParameters(Aws::Http::URI& uri) const override; 
 
    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override; 
 
 
    /** 
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
    inline UploadPartRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;} 
 
    /** 
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
    inline UploadPartRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;} 
 
    /** 
     * <p>The name of the bucket to which the multipart upload was initiated.</p> 
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
    inline UploadPartRequest& WithBucket(const char* value) { SetBucket(value); return *this;} 
 
 
    /** 
     * <p>Size of the body in bytes. This parameter is useful when the size of the body 
     * cannot be determined automatically.</p> 
     */ 
    inline long long GetContentLength() const{ return m_contentLength; } 
 
    /** 
     * <p>Size of the body in bytes. This parameter is useful when the size of the body 
     * cannot be determined automatically.</p> 
     */ 
    inline bool ContentLengthHasBeenSet() const { return m_contentLengthHasBeenSet; }

    /**
     * <p>Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically.</p>
     */
    inline void SetContentLength(long long value) { m_contentLengthHasBeenSet = true; m_contentLength = value; } 
 
    /** 
     * <p>Size of the body in bytes. This parameter is useful when the size of the body 
     * cannot be determined automatically.</p> 
     */ 
    inline UploadPartRequest& WithContentLength(long long value) { SetContentLength(value); return *this;} 
 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; } 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline bool ContentMD5HasBeenSet() const { return m_contentMD5HasBeenSet; }

    /**
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; } 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); } 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); } 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline UploadPartRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;} 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline UploadPartRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;} 
 
    /** 
     * <p>The base64-encoded 128-bit MD5 digest of the part data. This parameter is 
     * auto-populated when using the command from the CLI. This parameter is required 
     * if object lock parameters are specified.</p> 
     */ 
    inline UploadPartRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;} 
 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline const Aws::String& GetKey() const{ return m_key; } 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Object key for which the multipart upload was initiated.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; } 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); } 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); } 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline UploadPartRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;} 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline UploadPartRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;} 
 
    /** 
     * <p>Object key for which the multipart upload was initiated.</p> 
     */ 
    inline UploadPartRequest& WithKey(const char* value) { SetKey(value); return *this;} 
 
 
    /** 
     * <p>Part number of part being uploaded. This is a positive integer between 1 and 
     * 10,000.</p> 
     */ 
    inline int GetPartNumber() const{ return m_partNumber; } 
 
    /** 
     * <p>Part number of part being uploaded. This is a positive integer between 1 and 
     * 10,000.</p> 
     */ 
    inline bool PartNumberHasBeenSet() const { return m_partNumberHasBeenSet; }

    /**
     * <p>Part number of part being uploaded. This is a positive integer between 1 and
     * 10,000.</p>
     */
    inline void SetPartNumber(int value) { m_partNumberHasBeenSet = true; m_partNumber = value; } 
 
    /** 
     * <p>Part number of part being uploaded. This is a positive integer between 1 and 
     * 10,000.</p> 
     */ 
    inline UploadPartRequest& WithPartNumber(int value) { SetPartNumber(value); return *this;} 
 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline const Aws::String& GetUploadId() const{ return m_uploadId; } 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline bool UploadIdHasBeenSet() const { return m_uploadIdHasBeenSet; }

    /**
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p>
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadIdHasBeenSet = true; m_uploadId = value; } 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline void SetUploadId(Aws::String&& value) { m_uploadIdHasBeenSet = true; m_uploadId = std::move(value); } 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline void SetUploadId(const char* value) { m_uploadIdHasBeenSet = true; m_uploadId.assign(value); } 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline UploadPartRequest& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;} 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline UploadPartRequest& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;} 
 
    /** 
     * <p>Upload ID identifying the multipart upload whose part is being uploaded.</p> 
     */ 
    inline UploadPartRequest& WithUploadId(const char* value) { SetUploadId(value); return *this;} 
 
 
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
    inline UploadPartRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;} 
 
    /** 
     * <p>Specifies the algorithm to use to when encrypting the object (for example, 
     * AES256).</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;} 
 
    /** 
     * <p>Specifies the algorithm to use to when encrypting the object (for example, 
     * AES256).</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;} 
 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline const Aws::String& GetSSECustomerKey() const{ return m_sSECustomerKey; } 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline bool SSECustomerKeyHasBeenSet() const { return m_sSECustomerKeyHasBeenSet; }

    /**
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */
    inline void SetSSECustomerKey(const Aws::String& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = value; } 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline void SetSSECustomerKey(Aws::String&& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = std::move(value); } 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline void SetSSECustomerKey(const char* value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey.assign(value); } 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;} 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;} 
 
    /** 
     * <p>Specifies the customer-provided encryption key for Amazon S3 to use in 
     * encrypting data. This value is used to store the object and then it is 
     * discarded; Amazon S3 does not store the encryption key. The key must be 
     * appropriate for use with the algorithm specified in the 
     * <code>x-amz-server-side-encryption-customer-algorithm header</code>. This must 
     * be the same encryption key specified in the initiate multipart upload 
     * request.</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;} 
 
 
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
    inline UploadPartRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;} 
 
    /** 
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. 
     * Amazon S3 uses this header for a message integrity check to ensure that the 
     * encryption key was transmitted without error.</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;} 
 
    /** 
     * <p>Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. 
     * Amazon S3 uses this header for a message integrity check to ensure that the 
     * encryption key was transmitted without error.</p> 
     */ 
    inline UploadPartRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;} 
 
 
     
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; } 
 
     
    inline bool RequestPayerHasBeenSet() const { return m_requestPayerHasBeenSet; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; } 
 
     
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); } 
 
     
    inline UploadPartRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;} 
 
     
    inline UploadPartRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;} 
 
 
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
    inline UploadPartRequest& WithExpectedBucketOwner(const Aws::String& value) { SetExpectedBucketOwner(value); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline UploadPartRequest& WithExpectedBucketOwner(Aws::String&& value) { SetExpectedBucketOwner(std::move(value)); return *this;} 
 
    /** 
     * <p>The account id of the expected bucket owner. If the bucket is owned by a 
     * different account, the request will fail with an HTTP <code>403 (Access 
     * Denied)</code> error.</p> 
     */ 
    inline UploadPartRequest& WithExpectedBucketOwner(const char* value) { SetExpectedBucketOwner(value); return *this;} 
 
 
     
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; } 
 
     
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; } 
 
     
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); } 
 
     
    inline UploadPartRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;} 
 
     
    inline UploadPartRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;} 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; } 
 
     
    inline UploadPartRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; } 
 
  private: 
 
 
    Aws::String m_bucket; 
    bool m_bucketHasBeenSet; 
 
    long long m_contentLength; 
    bool m_contentLengthHasBeenSet; 
 
    Aws::String m_contentMD5; 
    bool m_contentMD5HasBeenSet; 
 
    Aws::String m_key; 
    bool m_keyHasBeenSet; 
 
    int m_partNumber; 
    bool m_partNumberHasBeenSet; 
 
    Aws::String m_uploadId; 
    bool m_uploadIdHasBeenSet; 
 
    Aws::String m_sSECustomerAlgorithm; 
    bool m_sSECustomerAlgorithmHasBeenSet; 
 
    Aws::String m_sSECustomerKey; 
    bool m_sSECustomerKeyHasBeenSet; 
 
    Aws::String m_sSECustomerKeyMD5; 
    bool m_sSECustomerKeyMD5HasBeenSet; 
 
    RequestPayer m_requestPayer; 
    bool m_requestPayerHasBeenSet; 
 
    Aws::String m_expectedBucketOwner; 
    bool m_expectedBucketOwnerHasBeenSet; 
 
    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag; 
    bool m_customizedAccessLogTagHasBeenSet; 
  }; 
 
} // namespace Model 
} // namespace S3 
} // namespace Aws 
