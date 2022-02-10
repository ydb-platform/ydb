/** 
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 * SPDX-License-Identifier: Apache-2.0. 
 */ 
 
#pragma once 
#include <aws/s3/S3_EXPORTS.h> 
#include <aws/core/utils/memory/stl/AWSVector.h> 
#include <aws/s3/model/ServerSideEncryptionRule.h> 
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
   * <p>Specifies the default server-side-encryption configuration.</p><p><h3>See 
   * Also:</h3>   <a 
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ServerSideEncryptionConfiguration">AWS 
   * API Reference</a></p> 
   */ 
  class AWS_S3_API ServerSideEncryptionConfiguration 
  { 
  public: 
    ServerSideEncryptionConfiguration(); 
    ServerSideEncryptionConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode); 
    ServerSideEncryptionConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode); 
 
    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const; 
 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline const Aws::Vector<ServerSideEncryptionRule>& GetRules() const{ return m_rules; } 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline bool RulesHasBeenSet() const { return m_rulesHasBeenSet; }

    /**
     * <p>Container for information about a particular server-side encryption
     * configuration rule.</p>
     */
    inline void SetRules(const Aws::Vector<ServerSideEncryptionRule>& value) { m_rulesHasBeenSet = true; m_rules = value; } 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline void SetRules(Aws::Vector<ServerSideEncryptionRule>&& value) { m_rulesHasBeenSet = true; m_rules = std::move(value); } 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline ServerSideEncryptionConfiguration& WithRules(const Aws::Vector<ServerSideEncryptionRule>& value) { SetRules(value); return *this;} 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline ServerSideEncryptionConfiguration& WithRules(Aws::Vector<ServerSideEncryptionRule>&& value) { SetRules(std::move(value)); return *this;} 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline ServerSideEncryptionConfiguration& AddRules(const ServerSideEncryptionRule& value) { m_rulesHasBeenSet = true; m_rules.push_back(value); return *this; } 
 
    /** 
     * <p>Container for information about a particular server-side encryption 
     * configuration rule.</p> 
     */ 
    inline ServerSideEncryptionConfiguration& AddRules(ServerSideEncryptionRule&& value) { m_rulesHasBeenSet = true; m_rules.push_back(std::move(value)); return *this; } 
 
  private: 
 
    Aws::Vector<ServerSideEncryptionRule> m_rules; 
    bool m_rulesHasBeenSet; 
  }; 
 
} // namespace Model 
} // namespace S3 
} // namespace Aws 
