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
   * <p>Object Identifier is unique value to identify objects.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ObjectIdentifier">AWS
   * API Reference</a></p>
   */
  class ObjectIdentifier
  {
  public:
    AWS_S3_API ObjectIdentifier();
    AWS_S3_API ObjectIdentifier(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API ObjectIdentifier& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline bool KeyHasBeenSet() const { return m_keyHasBeenSet; }

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline ObjectIdentifier& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline ObjectIdentifier& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>Key name of the object.</p>  <p>Replacement must be made for
     * object keys containing special characters (such as carriage returns) when using
     * XML requests. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline ObjectIdentifier& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline ObjectIdentifier& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline ObjectIdentifier& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>VersionId for the specific version of the object to delete.</p>
     */
    inline ObjectIdentifier& WithVersionId(const char* value) { SetVersionId(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet = false;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
