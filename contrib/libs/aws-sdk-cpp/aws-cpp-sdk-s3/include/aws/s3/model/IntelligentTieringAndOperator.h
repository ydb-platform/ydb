/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tag.h>
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
   * <p>A container for specifying S3 Intelligent-Tiering filters. The filters
   * determine the subset of objects to which the rule applies.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/IntelligentTieringAndOperator">AWS
   * API Reference</a></p>
   */
  class IntelligentTieringAndOperator
  {
  public:
    AWS_S3_API IntelligentTieringAndOperator();
    AWS_S3_API IntelligentTieringAndOperator(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API IntelligentTieringAndOperator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline bool PrefixHasBeenSet() const { return m_prefixHasBeenSet; }

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline IntelligentTieringAndOperator& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline IntelligentTieringAndOperator& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * <p>An object key name prefix that identifies the subset of objects to which the
     * configuration applies.</p>
     */
    inline IntelligentTieringAndOperator& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline bool TagsHasBeenSet() const { return m_tagsHasBeenSet; }

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tagsHasBeenSet = true; m_tags = value; }

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tagsHasBeenSet = true; m_tags = std::move(value); }

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline IntelligentTieringAndOperator& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline IntelligentTieringAndOperator& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline IntelligentTieringAndOperator& AddTags(const Tag& value) { m_tagsHasBeenSet = true; m_tags.push_back(value); return *this; }

    /**
     * <p>All of these tags must exist in the object's tag set in order for the
     * configuration to apply.</p>
     */
    inline IntelligentTieringAndOperator& AddTags(Tag&& value) { m_tagsHasBeenSet = true; m_tags.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet = false;

    Aws::Vector<Tag> m_tags;
    bool m_tagsHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
