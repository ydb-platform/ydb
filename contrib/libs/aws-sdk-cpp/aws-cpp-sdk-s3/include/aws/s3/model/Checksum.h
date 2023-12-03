/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  /**
   * <p>Contains all the possible checksum or digest values for an
   * object.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Checksum">AWS API
   * Reference</a></p>
   */
  class Checksum
  {
  public:
    AWS_S3_API Checksum();
    AWS_S3_API Checksum(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API Checksum& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


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
    inline bool ChecksumCRC32HasBeenSet() const { return m_checksumCRC32HasBeenSet; }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const Aws::String& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = value; }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(Aws::String&& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = std::move(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32(const char* value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32.assign(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32(const Aws::String& value) { SetChecksumCRC32(value); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32(Aws::String&& value) { SetChecksumCRC32(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32 checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32(const char* value) { SetChecksumCRC32(value); return *this;}


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
    inline bool ChecksumCRC32CHasBeenSet() const { return m_checksumCRC32CHasBeenSet; }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const Aws::String& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = value; }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(Aws::String&& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = std::move(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumCRC32C(const char* value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C.assign(value); }

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32C(const Aws::String& value) { SetChecksumCRC32C(value); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32C(Aws::String&& value) { SetChecksumCRC32C(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 32-bit CRC32C checksum of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumCRC32C(const char* value) { SetChecksumCRC32C(value); return *this;}


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
    inline bool ChecksumSHA1HasBeenSet() const { return m_checksumSHA1HasBeenSet; }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const Aws::String& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = value; }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(Aws::String&& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = std::move(value); }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA1(const char* value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1.assign(value); }

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA1(const Aws::String& value) { SetChecksumSHA1(value); return *this;}

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA1(Aws::String&& value) { SetChecksumSHA1(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 160-bit SHA-1 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA1(const char* value) { SetChecksumSHA1(value); return *this;}


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
    inline bool ChecksumSHA256HasBeenSet() const { return m_checksumSHA256HasBeenSet; }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const Aws::String& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = value; }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(Aws::String&& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = std::move(value); }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetChecksumSHA256(const char* value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256.assign(value); }

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA256(const Aws::String& value) { SetChecksumSHA256(value); return *this;}

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA256(Aws::String&& value) { SetChecksumSHA256(std::move(value)); return *this;}

    /**
     * <p>The base64-encoded, 256-bit SHA-256 digest of the object. This will only be
     * present if it was uploaded with the object. With multipart uploads, this may not
     * be a checksum value of the object. For more information about how checksums are
     * calculated with multipart uploads, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums">
     * Checking object integrity</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline Checksum& WithChecksumSHA256(const char* value) { SetChecksumSHA256(value); return *this;}

  private:

    Aws::String m_checksumCRC32;
    bool m_checksumCRC32HasBeenSet = false;

    Aws::String m_checksumCRC32C;
    bool m_checksumCRC32CHasBeenSet = false;

    Aws::String m_checksumSHA1;
    bool m_checksumSHA1HasBeenSet = false;

    Aws::String m_checksumSHA256;
    bool m_checksumSHA256HasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
