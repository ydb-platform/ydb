/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/ObjectStorageClass.h>
#include <aws/s3/model/Owner.h>
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
   * <p>An object consists of data and its descriptive metadata.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Object">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API Object
  {
  public:
    Object();
    Object(const Aws::Utils::Xml::XmlNode& xmlNode);
    Object& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline Object& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline Object& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>The name that you assign to an object. You use the object key to retrieve the
     * object.</p>
     */
    inline Object& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline bool LastModifiedHasBeenSet() const { return m_lastModifiedHasBeenSet; }

    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline Object& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * <p>The date the Object was Last Modified</p>
     */
    inline Object& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}


    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline bool ETagHasBeenSet() const { return m_eTagHasBeenSet; }

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline void SetETag(const Aws::String& value) { m_eTagHasBeenSet = true; m_eTag = value; }

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline void SetETag(Aws::String&& value) { m_eTagHasBeenSet = true; m_eTag = std::move(value); }

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline void SetETag(const char* value) { m_eTagHasBeenSet = true; m_eTag.assign(value); }

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline Object& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline Object& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * <p>The entity tag is a hash of the object. The ETag reflects changes only to the
     * contents of an object, not its metadata. The ETag may or may not be an MD5
     * digest of the object data. Whether or not it is depends on how the object was
     * created and how it is encrypted as described below:</p> <ul> <li> <p>Objects
     * created by the PUT Object, POST Object, or Copy operation, or through the AWS
     * Management Console, and are encrypted by SSE-S3 or plaintext, have ETags that
     * are an MD5 digest of their object data.</p> </li> <li> <p>Objects created by the
     * PUT Object, POST Object, or Copy operation, or through the AWS Management
     * Console, and are encrypted by SSE-C or SSE-KMS, have ETags that are not an MD5
     * digest of their object data.</p> </li> <li> <p>If an object is created by either
     * the Multipart Upload or Part Copy operation, the ETag is not an MD5 digest,
     * regardless of the method of encryption.</p> </li> </ul>
     */
    inline Object& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * <p>Size in bytes of the object</p>
     */
    inline long long GetSize() const{ return m_size; }

    /**
     * <p>Size in bytes of the object</p>
     */
    inline bool SizeHasBeenSet() const { return m_sizeHasBeenSet; }

    /**
     * <p>Size in bytes of the object</p>
     */
    inline void SetSize(long long value) { m_sizeHasBeenSet = true; m_size = value; }

    /**
     * <p>Size in bytes of the object</p>
     */
    inline Object& WithSize(long long value) { SetSize(value); return *this;}


    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline const ObjectStorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(const ObjectStorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline void SetStorageClass(ObjectStorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline Object& WithStorageClass(const ObjectStorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>The class of storage used to store the object.</p>
     */
    inline Object& WithStorageClass(ObjectStorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>The owner of the object</p>
     */
    inline const Owner& GetOwner() const{ return m_owner; }

    /**
     * <p>The owner of the object</p>
     */
    inline bool OwnerHasBeenSet() const { return m_ownerHasBeenSet; }

    /**
     * <p>The owner of the object</p>
     */
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    /**
     * <p>The owner of the object</p>
     */
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    /**
     * <p>The owner of the object</p>
     */
    inline Object& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    /**
     * <p>The owner of the object</p>
     */
    inline Object& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet;

    Aws::String m_eTag;
    bool m_eTagHasBeenSet;

    long long m_size;
    bool m_sizeHasBeenSet;

    ObjectStorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    Owner m_owner;
    bool m_ownerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
