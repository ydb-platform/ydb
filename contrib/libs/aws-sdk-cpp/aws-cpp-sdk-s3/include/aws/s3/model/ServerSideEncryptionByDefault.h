/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/ServerSideEncryption.h>
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
   * <p>Describes the default server-side encryption to apply to new objects in the
   * bucket. If a PUT Object request doesn't specify any server-side encryption, this
   * default encryption will be applied. For more information, see <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTencryption.html">PUT
   * Bucket encryption</a> in the <i>Amazon Simple Storage Service API
   * Reference</i>.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ServerSideEncryptionByDefault">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API ServerSideEncryptionByDefault
  {
  public:
    ServerSideEncryptionByDefault();
    ServerSideEncryptionByDefault(const Aws::Utils::Xml::XmlNode& xmlNode);
    ServerSideEncryptionByDefault& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline const ServerSideEncryption& GetSSEAlgorithm() const{ return m_sSEAlgorithm; }

    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline bool SSEAlgorithmHasBeenSet() const { return m_sSEAlgorithmHasBeenSet; }

    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline void SetSSEAlgorithm(const ServerSideEncryption& value) { m_sSEAlgorithmHasBeenSet = true; m_sSEAlgorithm = value; }

    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline void SetSSEAlgorithm(ServerSideEncryption&& value) { m_sSEAlgorithmHasBeenSet = true; m_sSEAlgorithm = std::move(value); }

    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline ServerSideEncryptionByDefault& WithSSEAlgorithm(const ServerSideEncryption& value) { SetSSEAlgorithm(value); return *this;}

    /**
     * <p>Server-side encryption algorithm to use for the default encryption.</p>
     */
    inline ServerSideEncryptionByDefault& WithSSEAlgorithm(ServerSideEncryption&& value) { SetSSEAlgorithm(std::move(value)); return *this;}


    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline const Aws::String& GetKMSMasterKeyID() const{ return m_kMSMasterKeyID; }

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline bool KMSMasterKeyIDHasBeenSet() const { return m_kMSMasterKeyIDHasBeenSet; }

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(const Aws::String& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = value; }

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(Aws::String&& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = std::move(value); }

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(const char* value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID.assign(value); }

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const Aws::String& value) { SetKMSMasterKeyID(value); return *this;}

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(Aws::String&& value) { SetKMSMasterKeyID(std::move(value)); return *this;}

    /**
     * <p>AWS Key Management Service (KMS) customer master key ID to use for the
     * default encryption. This parameter is allowed if and only if
     * <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can specify
     * the key ID or the Amazon Resource Name (ARN) of the CMK. However, if you are
     * using encryption with cross-account operations, you must use a fully qualified
     * CMK ARN. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric CMKs and not
     * asymmetric CMKs. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * Symmetric and Asymmetric Keys</a> in the <i>AWS Key Management Service Developer
     * Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const char* value) { SetKMSMasterKeyID(value); return *this;}

  private:

    ServerSideEncryption m_sSEAlgorithm;
    bool m_sSEAlgorithmHasBeenSet;

    Aws::String m_kMSMasterKeyID;
    bool m_kMSMasterKeyIDHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
