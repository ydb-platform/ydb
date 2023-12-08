/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Rule.h>
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
   * <p>Container for lifecycle rules. You can add as many as 1000
   * rules.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/LifecycleConfiguration">AWS
   * API Reference</a></p>
   */
  class LifecycleConfiguration
  {
  public:
    AWS_S3_API LifecycleConfiguration();
    AWS_S3_API LifecycleConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API LifecycleConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline const Aws::Vector<Rule>& GetRules() const{ return m_rules; }

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline bool RulesHasBeenSet() const { return m_rulesHasBeenSet; }

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline void SetRules(const Aws::Vector<Rule>& value) { m_rulesHasBeenSet = true; m_rules = value; }

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline void SetRules(Aws::Vector<Rule>&& value) { m_rulesHasBeenSet = true; m_rules = std::move(value); }

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline LifecycleConfiguration& WithRules(const Aws::Vector<Rule>& value) { SetRules(value); return *this;}

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline LifecycleConfiguration& WithRules(Aws::Vector<Rule>&& value) { SetRules(std::move(value)); return *this;}

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline LifecycleConfiguration& AddRules(const Rule& value) { m_rulesHasBeenSet = true; m_rules.push_back(value); return *this; }

    /**
     * <p>Specifies lifecycle configuration rules for an Amazon S3 bucket. </p>
     */
    inline LifecycleConfiguration& AddRules(Rule&& value) { m_rulesHasBeenSet = true; m_rules.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<Rule> m_rules;
    bool m_rulesHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
