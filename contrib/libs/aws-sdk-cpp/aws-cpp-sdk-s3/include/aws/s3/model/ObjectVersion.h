/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/ObjectVersionStorageClass.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/Owner.h>
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
   * <p>The version of an object.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ObjectVersion">AWS
   * API Reference</a></p>
   */
  class ObjectVersion
  {
  public:
    AWS_S3_API ObjectVersion();
    AWS_S3_API ObjectVersion(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API ObjectVersion& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline bool ETagHasBeenSet() const { return m_eTagHasBeenSet; }

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline void SetETag(const Aws::String& value) { m_eTagHasBeenSet = true; m_eTag = value; }

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline void SetETag(Aws::String&& value) { m_eTagHasBeenSet = true; m_eTag = std::move(value); }

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline void SetETag(const char* value) { m_eTagHasBeenSet = true; m_eTag.assign(value); }

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline ObjectVersion& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline ObjectVersion& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * <p>The entity tag is an MD5 hash of that version of the object.</p>
     */
    inline ObjectVersion& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline const Aws::Vector<ChecksumAlgorithm>& GetChecksumAlgorithm() const{ return m_checksumAlgorithm; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline bool ChecksumAlgorithmHasBeenSet() const { return m_checksumAlgorithmHasBeenSet; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline void SetChecksumAlgorithm(const Aws::Vector<ChecksumAlgorithm>& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = value; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline void SetChecksumAlgorithm(Aws::Vector<ChecksumAlgorithm>&& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm = std::move(value); }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline ObjectVersion& WithChecksumAlgorithm(const Aws::Vector<ChecksumAlgorithm>& value) { SetChecksumAlgorithm(value); return *this;}

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline ObjectVersion& WithChecksumAlgorithm(Aws::Vector<ChecksumAlgorithm>&& value) { SetChecksumAlgorithm(std::move(value)); return *this;}

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline ObjectVersion& AddChecksumAlgorithm(const ChecksumAlgorithm& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm.push_back(value); return *this; }

    /**
     * <p>The algorithm that was used to create a checksum of the object.</p>
     */
    inline ObjectVersion& AddChecksumAlgorithm(ChecksumAlgorithm&& value) { m_checksumAlgorithmHasBeenSet = true; m_checksumAlgorithm.push_back(std::move(value)); return *this; }


    /**
     * <p>Size in bytes of the object.</p>
     */
    inline long long GetSize() const{ return m_size; }

    /**
     * <p>Size in bytes of the object.</p>
     */
    inline bool SizeHasBeenSet() const { return m_sizeHasBeenSet; }

    /**
     * <p>Size in bytes of the object.</p>
     */
    inline void SetSize(long long value) { m_sizeHasBeenSet = true; m_size = value; }

    /**
     * <p>Size in bytes of the object.</p>
     */
    inline ObjectVersion& WithSize(long long value) { SetSize(value); return *this;}


    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline const ObjectVersionStorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(const ObjectVersionStorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(ObjectVersionStorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline ObjectVersion& WithStorageClass(const ObjectVersionStorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline ObjectVersion& WithStorageClass(ObjectVersionStorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>The object key.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>The object key.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>The object key.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>The object key.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>The object key.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>The object key.</p>
     */
    inline ObjectVersion& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>The object key.</p>
     */
    inline ObjectVersion& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>The object key.</p>
     */
    inline ObjectVersion& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>Version ID of an object.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>Version ID of an object.</p>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>Version ID of an object.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p>Version ID of an object.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p>Version ID of an object.</p>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p>Version ID of an object.</p>
     */
    inline ObjectVersion& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>Version ID of an object.</p>
     */
    inline ObjectVersion& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>Version ID of an object.</p>
     */
    inline ObjectVersion& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p>Specifies whether the object is (true) or is not (false) the latest version
     * of an object.</p>
     */
    inline bool GetIsLatest() const{ return m_isLatest; }

    /**
     * <p>Specifies whether the object is (true) or is not (false) the latest version
     * of an object.</p>
     */
    inline bool IsLatestHasBeenSet() const { return m_isLatestHasBeenSet; }

    /**
     * <p>Specifies whether the object is (true) or is not (false) the latest version
     * of an object.</p>
     */
    inline void SetIsLatest(bool value) { m_isLatestHasBeenSet = true; m_isLatest = value; }

    /**
     * <p>Specifies whether the object is (true) or is not (false) the latest version
     * of an object.</p>
     */
    inline ObjectVersion& WithIsLatest(bool value) { SetIsLatest(value); return *this;}


    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline bool LastModifiedHasBeenSet() const { return m_lastModifiedHasBeenSet; }

    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline ObjectVersion& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * <p>Date and time the object was last modified.</p>
     */
    inline ObjectVersion& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}


    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline bool OwnerHasBeenSet() const { return m_ownerHasBeenSet; }

    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline ObjectVersion& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p>Specifies the owner of the object.</p>
     */
    inline ObjectVersion& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}

  private:

    Aws::String m_eTag;
    bool m_eTagHasBeenSet = false;

    Aws::Vector<ChecksumAlgorithm> m_checksumAlgorithm;
    bool m_checksumAlgorithmHasBeenSet = false;

    long long m_size;
    bool m_sizeHasBeenSet = false;

    ObjectVersionStorageClass m_storageClass;
    bool m_storageClassHasBeenSet = false;

    Aws::String m_key;
    bool m_keyHasBeenSet = false;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet = false;

    bool m_isLatest;
    bool m_isLatestHasBeenSet = false;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet = false;

    Owner m_owner;
    bool m_ownerHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
