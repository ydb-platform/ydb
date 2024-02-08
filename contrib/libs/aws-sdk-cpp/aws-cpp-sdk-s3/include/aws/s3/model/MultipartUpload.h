/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/Owner.h>
#include <aws/s3/model/Initiator.h>
#include <aws/s3/model/ChecksumAlgorithm.h>
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
   * <p>Container for the <code>MultipartUpload</code> for the Amazon S3
   * object.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/MultipartUpload">AWS
   * API Reference</a></p>
   */
  class MultipartUpload
  {
  public:
    AWS_S3_API MultipartUpload();
    AWS_S3_API MultipartUpload(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API MultipartUpload& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline bool UploadIdHasBeenSet() const { return m_uploadIdHasBeenSet; }

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadIdHasBeenSet = true; m_uploadId = value; }

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadIdHasBeenSet = true; m_uploadId = std::move(value); }

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline void SetUploadId(const char* value) { m_uploadIdHasBeenSet = true; m_uploadId.assign(value); }

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline MultipartUpload& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline MultipartUpload& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * <p>Upload ID that identifies the multipart upload.</p>
     */
    inline MultipartUpload& WithUploadId(const char* value) { SetUploadId(value); return *this;}


    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline MultipartUpload& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline MultipartUpload& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>Key of the object for which the multipart upload was initiated.</p>
     */
    inline MultipartUpload& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline const Aws::Utils::DateTime& GetInitiated() const{ return m_initiated; }

    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline bool InitiatedHasBeenSet() const { return m_initiatedHasBeenSet; }

    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline void SetInitiated(const Aws::Utils::DateTime& value) { m_initiatedHasBeenSet = true; m_initiated = value; }

    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline void SetInitiated(Aws::Utils::DateTime&& value) { m_initiatedHasBeenSet = true; m_initiated = std::move(value); }

    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline MultipartUpload& WithInitiated(const Aws::Utils::DateTime& value) { SetInitiated(value); return *this;}

    /**
     * <p>Date and time at which the multipart upload was initiated.</p>
     */
    inline MultipartUpload& WithInitiated(Aws::Utils::DateTime&& value) { SetInitiated(std::move(value)); return *this;}


    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline MultipartUpload& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline MultipartUpload& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline bool OwnerHasBeenSet() const { return m_ownerHasBeenSet; }

    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline MultipartUpload& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p>Specifies the owner of the object that is part of the multipart upload. </p>
     */
    inline MultipartUpload& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline const Initiator& GetInitiator() const{ return m_initiator; }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline bool InitiatorHasBeenSet() const { return m_initiatorHasBeenSet; }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline void SetInitiator(const Initiator& value) { m_initiatorHasBeenSet = true; m_initiator = value; }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline void SetInitiator(Initiator&& value) { m_initiatorHasBeenSet = true; m_initiator = std::move(value); }

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline MultipartUpload& WithInitiator(const Initiator& value) { SetInitiator(value); return *this;}

    /**
     * <p>Identifies who initiated the multipart upload.</p>
     */
    inline MultipartUpload& WithInitiator(Initiator&& value) { SetInitiator(std::move(value)); return *this;}


    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline const ChecksumAlgorithm& GetChecksumAlgorithm() const{ return m_checksumAlgorithm; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline bool ChecksumAlgorithmHasBeenSet() const { return m_checksumAlgorithmHasBeenSet; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline void SetChecksumAlgorithm(const ChecksumAlgorithm& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = value; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline void SetChecksumAlgorithm(ChecksumAlgorithm&& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = std::move(value); }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline MultipartUpload& WithChecksumAlgorithm(const ChecksumAlgorithm& value) { SetChecksumAlgorithm(value); return *this;}

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline MultipartUpload& WithChecksumAlgorithm(ChecksumAlgorithm&& value) { SetChecksumAlgorithm(std::move(value)); return *this;}

  private:

    Aws::String m_uploadId;
    bool m_uploadIdHasBeenSet = false;

    Aws::String m_key;
    bool m_keyHasBeenSet = false;

    Aws::Utils::DateTime m_initiated;
    bool m_initiatedHasBeenSet = false;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet = false;

    Owner m_owner;
    bool m_ownerHasBeenSet = false;

    Initiator m_initiator;
    bool m_initiatorHasBeenSet = false;

    ChecksumAlgorithm m_checksumAlgorithm;
    bool m_checksumAlgorithmHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
