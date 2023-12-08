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
   * <p>A conjunction (logical AND) of predicates, which is used in evaluating a
   * metrics filter. The operator must have at least two predicates in any
   * combination, and an object must match all of the predicates for the filter to
   * apply.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/AnalyticsAndOperator">AWS
   * API Reference</a></p>
   */
  class AnalyticsAndOperator
  {
  public:
    AWS_S3_API AnalyticsAndOperator();
    AWS_S3_API AnalyticsAndOperator(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API AnalyticsAndOperator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline bool PrefixHasBeenSet() const { return m_prefixHasBeenSet; }

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline AnalyticsAndOperator& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline AnalyticsAndOperator& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * <p>The prefix to use when evaluating an AND predicate: The prefix that an object
     * must have to be included in the metrics results.</p>
     */
    inline AnalyticsAndOperator& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline bool TagsHasBeenSet() const { return m_tagsHasBeenSet; }

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tagsHasBeenSet = true; m_tags = value; }

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tagsHasBeenSet = true; m_tags = std::move(value); }

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline AnalyticsAndOperator& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline AnalyticsAndOperator& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline AnalyticsAndOperator& AddTags(const Tag& value) { m_tagsHasBeenSet = true; m_tags.push_back(value); return *this; }

    /**
     * <p>The list of tags to use when evaluating an AND predicate.</p>
     */
    inline AnalyticsAndOperator& AddTags(Tag&& value) { m_tagsHasBeenSet = true; m_tags.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet = false;

    Aws::Vector<Tag> m_tags;
    bool m_tagsHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
