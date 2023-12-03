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
   * <p>Contains the type of server-side encryption used.</p><p><h3>See Also:</h3>  
   * <a href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Encryption">AWS
   * API Reference</a></p>
   */
  class Encryption
  {
  public:
    AWS_S3_API Encryption();
    AWS_S3_API Encryption(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API Encryption& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline const ServerSideEncryption& GetEncryptionType() const{ return m_encryptionType; }

    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline bool EncryptionTypeHasBeenSet() const { return m_encryptionTypeHasBeenSet; }

    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetEncryptionType(const ServerSideEncryption& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = value; }

    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetEncryptionType(ServerSideEncryption&& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = std::move(value); }

    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline Encryption& WithEncryptionType(const ServerSideEncryption& value) { SetEncryptionType(value); return *this;}

    /**
     * <p>The server-side encryption algorithm used when storing job results in Amazon
     * S3 (for example, AES256, aws:kms).</p>
     */
    inline Encryption& WithEncryptionType(ServerSideEncryption&& value) { SetEncryptionType(std::move(value)); return *this;}


    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline const Aws::String& GetKMSKeyId() const{ return m_kMSKeyId; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline bool KMSKeyIdHasBeenSet() const { return m_kMSKeyIdHasBeenSet; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline void SetKMSKeyId(const Aws::String& value) { m_kMSKeyIdHasBeenSet = true; m_kMSKeyId = value; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline void SetKMSKeyId(Aws::String&& value) { m_kMSKeyIdHasBeenSet = true; m_kMSKeyId = std::move(value); }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline void SetKMSKeyId(const char* value) { m_kMSKeyIdHasBeenSet = true; m_kMSKeyId.assign(value); }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline Encryption& WithKMSKeyId(const Aws::String& value) { SetKMSKeyId(value); return *this;}

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline Encryption& WithKMSKeyId(Aws::String&& value) { SetKMSKeyId(std::move(value)); return *this;}

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value specifies
     * the ID of the symmetric customer managed key to use for encryption of job
     * results. Amazon S3 only supports symmetric keys. For more information, see <a
     * href="https://docs.aws.amazon.com/kms/latest/developerguide/symmetric-asymmetric.html">Using
     * symmetric and asymmetric keys</a> in the <i>Amazon Web Services Key Management
     * Service Developer Guide</i>.</p>
     */
    inline Encryption& WithKMSKeyId(const char* value) { SetKMSKeyId(value); return *this;}


    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline const Aws::String& GetKMSContext() const{ return m_kMSContext; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline bool KMSContextHasBeenSet() const { return m_kMSContextHasBeenSet; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline void SetKMSContext(const Aws::String& value) { m_kMSContextHasBeenSet = true; m_kMSContext = value; }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline void SetKMSContext(Aws::String&& value) { m_kMSContextHasBeenSet = true; m_kMSContext = std::move(value); }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline void SetKMSContext(const char* value) { m_kMSContextHasBeenSet = true; m_kMSContext.assign(value); }

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline Encryption& WithKMSContext(const Aws::String& value) { SetKMSContext(value); return *this;}

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline Encryption& WithKMSContext(Aws::String&& value) { SetKMSContext(std::move(value)); return *this;}

    /**
     * <p>If the encryption type is <code>aws:kms</code>, this optional value can be
     * used to specify the encryption context for the restore results.</p>
     */
    inline Encryption& WithKMSContext(const char* value) { SetKMSContext(value); return *this;}

  private:

    ServerSideEncryption m_encryptionType;
    bool m_encryptionTypeHasBeenSet = false;

    Aws::String m_kMSKeyId;
    bool m_kMSKeyIdHasBeenSet = false;

    Aws::String m_kMSContext;
    bool m_kMSContextHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
