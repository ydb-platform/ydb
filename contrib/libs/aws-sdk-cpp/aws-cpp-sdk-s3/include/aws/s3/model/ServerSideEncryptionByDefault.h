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
   * default encryption will be applied. If you don't specify a customer managed key
   * at configuration, Amazon S3 automatically creates an Amazon Web Services KMS key
   * in your Amazon Web Services account the first time that you add an object
   * encrypted with SSE-KMS to a bucket. By default, Amazon S3 uses this KMS key for
   * SSE-KMS. For more information, see <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTencryption.html">PUT
   * Bucket encryption</a> in the <i>Amazon S3 API Reference</i>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ServerSideEncryptionByDefault">AWS
   * API Reference</a></p>
   */
  class ServerSideEncryptionByDefault
  {
  public:
    AWS_S3_API ServerSideEncryptionByDefault();
    AWS_S3_API ServerSideEncryptionByDefault(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API ServerSideEncryptionByDefault& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


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
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline const Aws::String& GetKMSMasterKeyID() const{ return m_kMSMasterKeyID; }

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline bool KMSMasterKeyIDHasBeenSet() const { return m_kMSMasterKeyIDHasBeenSet; }

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(const Aws::String& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = value; }

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(Aws::String&& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = std::move(value); }

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline void SetKMSMasterKeyID(const char* value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID.assign(value); }

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const Aws::String& value) { SetKMSMasterKeyID(value); return *this;}

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(Aws::String&& value) { SetKMSMasterKeyID(std::move(value)); return *this;}

    /**
     * <p>Amazon Web Services Key Management Service (KMS) customer Amazon Web Services
     * KMS key ID to use for the default encryption. This parameter is allowed if and
     * only if <code>SSEAlgorithm</code> is set to <code>aws:kms</code>.</p> <p>You can
     * specify the key ID or the Amazon Resource Name (ARN) of the KMS key. However, if
     * you are using encryption with cross-account or Amazon Web Services service
     * operations you must use a fully qualified KMS key ARN. For more information, see
     * <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html#bucket-encryption-update-bucket-policy">Using
     * encryption for cross-account operations</a>. </p> <p> <b>For example:</b> </p>
     * <ul> <li> <p>Key ID: <code>1234abcd-12ab-34cd-56ef-1234567890ab</code> </p>
     * </li> <li> <p>Key ARN:
     * <code>arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab</code>
     * </p> </li> </ul>  <p>Amazon S3 only supports symmetric KMS keys and
     * not asymmetric KMS keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p> 
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const char* value) { SetKMSMasterKeyID(value); return *this;}

  private:

    ServerSideEncryption m_sSEAlgorithm;
    bool m_sSEAlgorithmHasBeenSet = false;

    Aws::String m_kMSMasterKeyID;
    bool m_kMSMasterKeyIDHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
