/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/DefaultRetention.h>
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
   * <p>The container element for an Object Lock rule.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ObjectLockRule">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API ObjectLockRule
  {
  public:
    ObjectLockRule();
    ObjectLockRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    ObjectLockRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline const DefaultRetention& GetDefaultRetention() const{ return m_defaultRetention; }

    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline bool DefaultRetentionHasBeenSet() const { return m_defaultRetentionHasBeenSet; }

    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline void SetDefaultRetention(const DefaultRetention& value) { m_defaultRetentionHasBeenSet = true; m_defaultRetention = value; }

    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline void SetDefaultRetention(DefaultRetention&& value) { m_defaultRetentionHasBeenSet = true; m_defaultRetention = std::move(value); }

    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline ObjectLockRule& WithDefaultRetention(const DefaultRetention& value) { SetDefaultRetention(value); return *this;}

    /**
     * <p>The default retention period that you want to apply to new objects placed in
     * the specified bucket.</p>
     */
    inline ObjectLockRule& WithDefaultRetention(DefaultRetention&& value) { SetDefaultRetention(std::move(value)); return *this;}

  private:

    DefaultRetention m_defaultRetention;
    bool m_defaultRetentionHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
