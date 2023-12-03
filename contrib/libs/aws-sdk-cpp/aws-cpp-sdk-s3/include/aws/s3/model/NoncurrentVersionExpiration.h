/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>

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
   * <p>Specifies when noncurrent object versions expire. Upon expiration, Amazon S3
   * permanently deletes the noncurrent object versions. You set this lifecycle
   * configuration action on a bucket that has versioning enabled (or suspended) to
   * request that Amazon S3 delete noncurrent object versions at a specific period in
   * the object's lifetime.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/NoncurrentVersionExpiration">AWS
   * API Reference</a></p>
   */
  class NoncurrentVersionExpiration
  {
  public:
    AWS_S3_API NoncurrentVersionExpiration();
    AWS_S3_API NoncurrentVersionExpiration(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API NoncurrentVersionExpiration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. The value must be a non-zero positive integer.
     * For information about the noncurrent days calculations, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#non-current-days-calculations">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the <i>Amazon S3
     * User Guide</i>.</p>
     */
    inline int GetNoncurrentDays() const{ return m_noncurrentDays; }

    /**
     * <p>Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. The value must be a non-zero positive integer.
     * For information about the noncurrent days calculations, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#non-current-days-calculations">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the <i>Amazon S3
     * User Guide</i>.</p>
     */
    inline bool NoncurrentDaysHasBeenSet() const { return m_noncurrentDaysHasBeenSet; }

    /**
     * <p>Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. The value must be a non-zero positive integer.
     * For information about the noncurrent days calculations, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#non-current-days-calculations">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the <i>Amazon S3
     * User Guide</i>.</p>
     */
    inline void SetNoncurrentDays(int value) { m_noncurrentDaysHasBeenSet = true; m_noncurrentDays = value; }

    /**
     * <p>Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. The value must be a non-zero positive integer.
     * For information about the noncurrent days calculations, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#non-current-days-calculations">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the <i>Amazon S3
     * User Guide</i>.</p>
     */
    inline NoncurrentVersionExpiration& WithNoncurrentDays(int value) { SetNoncurrentDays(value); return *this;}


    /**
     * <p>Specifies how many noncurrent versions Amazon S3 will retain. If there are
     * this many more recent noncurrent versions, Amazon S3 will take the associated
     * action. For more information about noncurrent versions, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html">Lifecycle
     * configuration elements</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline int GetNewerNoncurrentVersions() const{ return m_newerNoncurrentVersions; }

    /**
     * <p>Specifies how many noncurrent versions Amazon S3 will retain. If there are
     * this many more recent noncurrent versions, Amazon S3 will take the associated
     * action. For more information about noncurrent versions, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html">Lifecycle
     * configuration elements</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline bool NewerNoncurrentVersionsHasBeenSet() const { return m_newerNoncurrentVersionsHasBeenSet; }

    /**
     * <p>Specifies how many noncurrent versions Amazon S3 will retain. If there are
     * this many more recent noncurrent versions, Amazon S3 will take the associated
     * action. For more information about noncurrent versions, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html">Lifecycle
     * configuration elements</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline void SetNewerNoncurrentVersions(int value) { m_newerNoncurrentVersionsHasBeenSet = true; m_newerNoncurrentVersions = value; }

    /**
     * <p>Specifies how many noncurrent versions Amazon S3 will retain. If there are
     * this many more recent noncurrent versions, Amazon S3 will take the associated
     * action. For more information about noncurrent versions, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html">Lifecycle
     * configuration elements</a> in the <i>Amazon S3 User Guide</i>.</p>
     */
    inline NoncurrentVersionExpiration& WithNewerNoncurrentVersions(int value) { SetNewerNoncurrentVersions(value); return *this;}

  private:

    int m_noncurrentDays;
    bool m_noncurrentDaysHasBeenSet = false;

    int m_newerNoncurrentVersions;
    bool m_newerNoncurrentVersionsHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
