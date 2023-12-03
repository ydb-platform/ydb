/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/Tag.h>
#include <aws/s3/model/LifecycleRuleAndOperator.h>
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
   * <p>The <code>Filter</code> is used to identify objects that a Lifecycle Rule
   * applies to. A <code>Filter</code> must have exactly one of <code>Prefix</code>,
   * <code>Tag</code>, or <code>And</code> specified.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/LifecycleRuleFilter">AWS
   * API Reference</a></p>
   */
  class LifecycleRuleFilter
  {
  public:
    AWS_S3_API LifecycleRuleFilter();
    AWS_S3_API LifecycleRuleFilter(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API LifecycleRuleFilter& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline bool PrefixHasBeenSet() const { return m_prefixHasBeenSet; }

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline LifecycleRuleFilter& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline LifecycleRuleFilter& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * <p>Prefix identifying one or more objects to which the rule applies.</p>
     *  <p>Replacement must be made for object keys containing special
     * characters (such as carriage returns) when using XML requests. For more
     * information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints">
     * XML related object key constraints</a>.</p> 
     */
    inline LifecycleRuleFilter& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline const Tag& GetTag() const{ return m_tag; }

    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline bool TagHasBeenSet() const { return m_tagHasBeenSet; }

    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline void SetTag(const Tag& value) { m_tagHasBeenSet = true; m_tag = value; }

    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline void SetTag(Tag&& value) { m_tagHasBeenSet = true; m_tag = std::move(value); }

    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline LifecycleRuleFilter& WithTag(const Tag& value) { SetTag(value); return *this;}

    /**
     * <p>This tag must exist in the object's tag set in order for the rule to
     * apply.</p>
     */
    inline LifecycleRuleFilter& WithTag(Tag&& value) { SetTag(std::move(value)); return *this;}


    /**
     * <p>Minimum object size to which the rule applies.</p>
     */
    inline long long GetObjectSizeGreaterThan() const{ return m_objectSizeGreaterThan; }

    /**
     * <p>Minimum object size to which the rule applies.</p>
     */
    inline bool ObjectSizeGreaterThanHasBeenSet() const { return m_objectSizeGreaterThanHasBeenSet; }

    /**
     * <p>Minimum object size to which the rule applies.</p>
     */
    inline void SetObjectSizeGreaterThan(long long value) { m_objectSizeGreaterThanHasBeenSet = true; m_objectSizeGreaterThan = value; }

    /**
     * <p>Minimum object size to which the rule applies.</p>
     */
    inline LifecycleRuleFilter& WithObjectSizeGreaterThan(long long value) { SetObjectSizeGreaterThan(value); return *this;}


    /**
     * <p>Maximum object size to which the rule applies.</p>
     */
    inline long long GetObjectSizeLessThan() const{ return m_objectSizeLessThan; }

    /**
     * <p>Maximum object size to which the rule applies.</p>
     */
    inline bool ObjectSizeLessThanHasBeenSet() const { return m_objectSizeLessThanHasBeenSet; }

    /**
     * <p>Maximum object size to which the rule applies.</p>
     */
    inline void SetObjectSizeLessThan(long long value) { m_objectSizeLessThanHasBeenSet = true; m_objectSizeLessThan = value; }

    /**
     * <p>Maximum object size to which the rule applies.</p>
     */
    inline LifecycleRuleFilter& WithObjectSizeLessThan(long long value) { SetObjectSizeLessThan(value); return *this;}


    
    inline const LifecycleRuleAndOperator& GetAnd() const{ return m_and; }

    
    inline bool AndHasBeenSet() const { return m_andHasBeenSet; }

    
    inline void SetAnd(const LifecycleRuleAndOperator& value) { m_andHasBeenSet = true; m_and = value; }

    
    inline void SetAnd(LifecycleRuleAndOperator&& value) { m_andHasBeenSet = true; m_and = std::move(value); }

    
    inline LifecycleRuleFilter& WithAnd(const LifecycleRuleAndOperator& value) { SetAnd(value); return *this;}

    
    inline LifecycleRuleFilter& WithAnd(LifecycleRuleAndOperator&& value) { SetAnd(std::move(value)); return *this;}

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet = false;

    Tag m_tag;
    bool m_tagHasBeenSet = false;

    long long m_objectSizeGreaterThan;
    bool m_objectSizeGreaterThanHasBeenSet = false;

    long long m_objectSizeLessThan;
    bool m_objectSizeLessThanHasBeenSet = false;

    LifecycleRuleAndOperator m_and;
    bool m_andHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
