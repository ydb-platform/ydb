/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/s3/model/RequestCharged.h>
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
  class CompleteMultipartUploadResult
  {
  public:
    AWS_S3_API CompleteMultipartUploadResult();
    AWS_S3_API CompleteMultipartUploadResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API CompleteMultipartUploadResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline const Aws::String& GetLocation() const{ return m_location; }

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline void SetLocation(const Aws::String& value) { m_location = value; }

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline void SetLocation(Aws::String&& value) { m_location = std::move(value); }

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline void SetLocation(const char* value) { m_location.assign(value); }

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithLocation(const Aws::String& value) { SetLocation(value); return *this;}

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithLocation(Aws::String&& value) { SetLocation(std::move(value)); return *this;}

    /**
     * <p>The URI that identifies the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithLocation(const char* value) { SetLocation(value); return *this;}


    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline void SetBucket(const Aws::String& value) { m_bucket = value; }

    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline void SetBucket(Aws::String&& value) { m_bucket = std::move(value); }

    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline void SetBucket(const char* value) { m_bucket.assign(value); }

    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline CompleteMultipartUploadResult& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline CompleteMultipartUploadResult& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * <p>The name of the bucket that contains the newly created object. Does not
     * return the access point ARN or access point alias if used.</p> <p>When using
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
    inline CompleteMultipartUploadResult& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * <p>The object key of the newly created object.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline void SetKey(const Aws::String& value) { m_key = value; }

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline void SetKey(Aws::String&& value) { m_key = std::move(value); }

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline void SetKey(const char* value) { m_key.assign(value); }

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>The object key of the newly created object.</p>
     */
    inline CompleteMultipartUploadResult& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline const Aws::String& GetExpiration() const{ return m_expiration; }

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline void SetExpiration(const Aws::String& value) { m_expiration = value; }

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline void SetExpiration(Aws::String&& value) { m_expiration = std::move(value); }

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline void SetExpiration(const char* value) { m_expiration.assign(value); }

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline CompleteMultipartUploadResult& WithExpiration(const Aws::String& value) { SetExpiration(value); return *this;}

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline CompleteMultipartUploadResult& WithExpiration(Aws::String&& value) { SetExpiration(std::move(value)); return *this;}

    /**
     * <p>If the object expiration is configured, this will contain the expiration date
     * (<code>expiry-date</code>) and rule ID (<code>rule-id</code>). The value of
     * <code>rule-id</code> is URL-encoded.</p>
     */
    inline CompleteMultipartUploadResult& WithExpiration(const char* value) { SetExpiration(value); return *this;}


    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetETag(const Aws::String& value) { m_eTag = value; }

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetETag(Aws::String&& value) { m_eTag = std::move(value); }

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetETag(const char* value) { m_eTag.assign(value); }

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * <p>Entity tag that identifies the newly created object's data. Objects with
     * different object data will have different entity tags. The entity tag is an
     * opaque string. The entity tag may or may not be an MD5 digest of the object
     * data. If the entity tag is not an MD5 digest of the object data, it will contain
     * one or more nonhexadecimal characters and/or will consist of less than 32 or
     * more than 32 hexadecimal digits. For more information about how the entity tag
     * is calculated, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumCRC32() const{ return m_checksumCRC32; }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const Aws::String& value) { m_checksumCRC32 = value; }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(Aws::String&& value) { m_checksumCRC32 = std::move(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const char* value) { m_checksumCRC32.assign(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32(const Aws::String& value) { SetChecksumCRC32(value); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32(Aws::String&& value) { SetChecksumCRC32(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32(const char* value) { SetChecksumCRC32(value); return *this;}


    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumCRC32C() const{ return m_checksumCRC32C; }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const Aws::String& value) { m_checksumCRC32C = value; }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(Aws::String&& value) { m_checksumCRC32C = std::move(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const char* value) { m_checksumCRC32C.assign(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32C(const Aws::String& value) { SetChecksumCRC32C(value); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32C(Aws::String&& value) { SetChecksumCRC32C(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumCRC32C(const char* value) { SetChecksumCRC32C(value); return *this;}


    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumSHA1() const{ return m_checksumSHA1; }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const Aws::String& value) { m_checksumSHA1 = value; }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(Aws::String&& value) { m_checksumSHA1 = std::move(value); }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const char* value) { m_checksumSHA1.assign(value); }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA1(const Aws::String& value) { SetChecksumSHA1(value); return *this;}

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA1(Aws::String&& value) { SetChecksumSHA1(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA1(const char* value) { SetChecksumSHA1(value); return *this;}


    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline const Aws::String& GetChecksumSHA256() const{ return m_checksumSHA256; }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const Aws::String& value) { m_checksumSHA256 = value; }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(Aws::String&& value) { m_checksumSHA256 = std::move(value); }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const char* value) { m_checksumSHA256.assign(value); }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA256(const Aws::String& value) { SetChecksumSHA256(value); return *this;}

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA256(Aws::String&& value) { SetChecksumSHA256(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline CompleteMultipartUploadResult& WithChecksumSHA256(const char* value) { SetChecksumSHA256(value); return *this;}


    /**
     * <p>If you specified server-side encryption either with an Amazon S3-managed
     * encryption key or an Amazon Web Services KMS key in your initiate multipart
     * upload request, the response includes this header. It confirms the encryption
     * algorithm that Amazon S3 used to encrypt the object.</p>
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * <p>If you specified server-side encryption either with an Amazon S3-managed
     * encryption key or an Amazon Web Services KMS key in your initiate multipart
     * upload request, the response includes this header. It confirms the encryption
     * algorithm that Amazon S3 used to encrypt the object.</p>
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryption = value; }

    /**
     * <p>If you specified server-side encryption either with an Amazon S3-managed
     * encryption key or an Amazon Web Services KMS key in your initiate multipart
     * upload request, the response includes this header. It confirms the encryption
     * algorithm that Amazon S3 used to encrypt the object.</p>
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryption = std::move(value); }

    /**
     * <p>If you specified server-side encryption either with an Amazon S3-managed
     * encryption key or an Amazon Web Services KMS key in your initiate multipart
     * upload request, the response includes this header. It confirms the encryption
     * algorithm that Amazon S3 used to encrypt the object.</p>
     */
    inline CompleteMultipartUploadResult& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * <p>If you specified server-side encryption either with an Amazon S3-managed
     * encryption key or an Amazon Web Services KMS key in your initiate multipart
     * upload request, the response includes this header. It confirms the encryption
     * algorithm that Amazon S3 used to encrypt the object.</p>
     */
    inline CompleteMultipartUploadResult& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline CompleteMultipartUploadResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline CompleteMultipartUploadResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>Version ID of the newly created object, in case the bucket has versioning
     * turned on.</p>
     */
    inline CompleteMultipartUploadResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyId = value; }

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyId = std::move(value); }

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyId.assign(value); }

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * <p>If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for the object.</p>
     */
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    /**
     * <p>Indicates whether the multipart upload uses an S3 Bucket Key for server-side
     * encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline bool GetBucketKeyEnabled() const{ return m_bucketKeyEnabled; }

    /**
     * <p>Indicates whether the multipart upload uses an S3 Bucket Key for server-side
     * encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline void SetBucketKeyEnabled(bool value) { m_bucketKeyEnabled = value; }

    /**
     * <p>Indicates whether the multipart upload uses an S3 Bucket Key for server-side
     * encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline CompleteMultipartUploadResult& WithBucketKeyEnabled(bool value) { SetBucketKeyEnabled(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline CompleteMultipartUploadResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline CompleteMultipartUploadResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    Aws::String m_location;

    Aws::String m_bucket;

    Aws::String m_key;

    Aws::String m_expiration;

    Aws::String m_eTag;

    Aws::String m_checksumCRC32;

    Aws::String m_checksumCRC32C;

    Aws::String m_checksumSHA1;

    Aws::String m_checksumSHA256;

    ServerSideEncryption m_serverSideEncryption;

    Aws::String m_versionId;

    Aws::String m_sSEKMSKeyId;

    bool m_bucketKeyEnabled;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
