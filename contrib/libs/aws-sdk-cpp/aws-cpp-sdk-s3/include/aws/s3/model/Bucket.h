/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
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
   * <p> In terms of implementation, a Bucket is a resource. An Amazon S3 bucket name
   * is globally unique, and the namespace is shared by all Amazon Web Services
   * accounts. </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Bucket">AWS API
   * Reference</a></p>
   */
  class Bucket
  {
  public:
    AWS_S3_API Bucket();
    AWS_S3_API Bucket(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API Bucket& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The name of the bucket.</p>
     */
    inline const Aws::String& GetName() const{ return m_name; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline bool NameHasBeenSet() const { return m_nameHasBeenSet; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetName(const Aws::String& value) { m_nameHasBeenSet = true; m_name = value; }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetName(Aws::String&& value) { m_nameHasBeenSet = true; m_name = std::move(value); }

    /**
     * <p>The name of the bucket.</p>
     */
    inline void SetName(const char* value) { m_nameHasBeenSet = true; m_name.assign(value); }

    /**
     * <p>The name of the bucket.</p>
     */
    inline Bucket& WithName(const Aws::String& value) { SetName(value); return *this;}

    /**
     * <p>The name of the bucket.</p>
     */
    inline Bucket& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}

    /**
     * <p>The name of the bucket.</p>
     */
    inline Bucket& WithName(const char* value) { SetName(value); return *this;}


    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline const Aws::Utils::DateTime& GetCreationDate() const{ return m_creationDate; }

    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline bool CreationDateHasBeenSet() const { return m_creationDateHasBeenSet; }

    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline void SetCreationDate(const Aws::Utils::DateTime& value) { m_creationDateHasBeenSet = true; m_creationDate = value; }

    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline void SetCreationDate(Aws::Utils::DateTime&& value) { m_creationDateHasBeenSet = true; m_creationDate = std::move(value); }

    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline Bucket& WithCreationDate(const Aws::Utils::DateTime& value) { SetCreationDate(value); return *this;}

    /**
     * <p>Date the bucket was created. This date can change when making changes to your
     * bucket, such as editing its bucket policy.</p>
     */
    inline Bucket& WithCreationDate(Aws::Utils::DateTime&& value) { SetCreationDate(std::move(value)); return *this;}

  private:

    Aws::String m_name;
    bool m_nameHasBeenSet = false;

    Aws::Utils::DateTime m_creationDate;
    bool m_creationDateHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
