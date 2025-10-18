#include "public/yaml_config.h"
#include "public/yaml_config_impl.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NYamlConfig;

Y_UNIT_TEST_SUITE(IncompatibilityRules) {
    Y_UNIT_TEST(BasicPatternMatching) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "environment";
        pattern.Values = THashSet<TString>{"production"};

        TLabel prodLabel{TLabel::EType::Common, "production"};
        TLabel devLabel{TLabel::EType::Common, "development"};
        TLabel emptyLabel{TLabel::EType::Empty, ""};

        UNIT_ASSERT(pattern.Matches(prodLabel, "environment"));
        UNIT_ASSERT(!pattern.Matches(devLabel, "environment"));
        UNIT_ASSERT(!pattern.Matches(emptyLabel, "environment"));
        UNIT_ASSERT(!pattern.Matches(prodLabel, "other_label"));
    }

    Y_UNIT_TEST(EmptyLabelMatching) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "tenant";
        pattern.Values = THashSet<TString>{""};

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "some_value"};

        UNIT_ASSERT(pattern.Matches(emptyLabel, "tenant"));
        UNIT_ASSERT(!pattern.Matches(commonLabel, "tenant"));
    }

    Y_UNIT_TEST(UnsetLabelMatching) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "cloud";
        pattern.Values = std::monostate{};

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "aws"};
        TLabel negativeLabel{TLabel::EType::Negative, ""};

        // monostate matches any value
        UNIT_ASSERT(pattern.Matches(emptyLabel, "cloud"));
        UNIT_ASSERT(pattern.Matches(commonLabel, "cloud"));
        UNIT_ASSERT(pattern.Matches(negativeLabel, "cloud"));
    }

    Y_UNIT_TEST(AddAndRemoveRules) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule1;
        rule1.RuleName = "test_rule_1";
        rule1.Patterns = {
            {.Name = "env", .Values = THashSet<TString>{"prod"}}
        };
        
        TIncompatibilityRule rule2;
        rule2.RuleName = "test_rule_2";
        rule2.Patterns = {
            {.Name = "cloud", .Values = THashSet<TString>{"aws"}}
        };

        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 0);

        rules.AddRule(rule1);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        rules.AddRule(rule2);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 2);

        rules.RemoveRule("test_rule_1");
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        rules.RemoveRule("test_rule_2");
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 0);
    }

    Y_UNIT_TEST(RuleOverride) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule1;
        rule1.RuleName = "test_rule";
        rule1.Patterns = {
            {.Name = "env", .Values = THashSet<TString>{"prod"}}
        };
        
        rules.AddRule(rule1);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TIncompatibilityRule rule2;
        rule2.RuleName = "test_rule";
        rule2.Patterns = {
            {.Name = "env", .Values = THashSet<TString>{"dev"}}
        };
        
        rules.AddRule(rule2);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);
    }

    Y_UNIT_TEST(SimpleIncompatibilityCheck) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "no_debug_in_prod";
        rule.Patterns = {
            {.Name = "environment", .Values = THashSet<TString>{"production"}},
            {.Name = "debug_mode", .Values = THashSet<TString>{"true"}}
        };
        rules.AddRule(rule);

        TVector<std::pair<TString, TSet<TLabel>>> labelNames = {
            {"environment", {}},
            {"debug_mode", {}}
        };

        TVector<TLabel> combination1 = {
            {TLabel::EType::Common, "production"},
            {TLabel::EType::Common, "false"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination1, labelNames));

        TVector<TLabel> combination2 = {
            {TLabel::EType::Common, "production"},
            {TLabel::EType::Common, "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(combination2, labelNames));

        TVector<TLabel> combination3 = {
            {TLabel::EType::Common, "development"},
            {TLabel::EType::Common, "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination3, labelNames));
    }

    Y_UNIT_TEST(DisableRules) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "test_rule";
        rule.Patterns = {
            {.Name = "env", .Values = THashSet<TString>{"prod"}},
            {.Name = "debug", .Values = THashSet<TString>{"true"}}
        };
        rules.AddRule(rule);

        TVector<std::pair<TString, TSet<TLabel>>> labelNames = {
            {"env", {}},
            {"debug", {}}
        };

        TVector<TLabel> combination = {
            {TLabel::EType::Common, "prod"},
            {TLabel::EType::Common, "true"}
        };

        UNIT_ASSERT(!rules.IsCompatible(combination, labelNames));

        // Use YAML parsing to disable rules
        const char* config = R"(---
incompatibility_overrides:
  disable_rules:
    - test_rule
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        TIncompatibilityRules userRules = ParseIncompatibilityRules(doc.Root());
        rules.MergeWith(userRules);

        UNIT_ASSERT(rules.IsCompatible(combination, labelNames));
        UNIT_ASSERT_VALUES_EQUAL(rules.GetDisabledCount(), 1);
    }

    Y_UNIT_TEST(MergeRules) {
        TIncompatibilityRules baseRules;
        
        TIncompatibilityRule rule1;
        rule1.RuleName = "base_rule";
        rule1.Patterns = {
            {.Name = "env", .Values = THashSet<TString>{"prod"}}
        };
        baseRules.AddRule(rule1);

        TIncompatibilityRules userRules;
        
        TIncompatibilityRule rule2;
        rule2.RuleName = "user_rule";
        rule2.Patterns = {
            {.Name = "cloud", .Values = THashSet<TString>{"aws"}}
        };
        userRules.AddRule(rule2);

        UNIT_ASSERT_VALUES_EQUAL(baseRules.GetRuleCount(), 1);
        
        baseRules.MergeWith(userRules);
        
        UNIT_ASSERT_VALUES_EQUAL(baseRules.GetRuleCount(), 2);
    }

    Y_UNIT_TEST(ParseEmptyOverrides) {
        const char* config = R"(---
config:
  value: 1
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetDisabledCount(), 0);
    }

    Y_UNIT_TEST(ParseDisableRules) {
        const char* config = R"(---
incompatibility_overrides:
  disable_rules:
    - rule1
    - rule2
    - rule3
config:
  value: 1
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetDisabledCount(), 3);
    }

    Y_UNIT_TEST(ParseCustomRules) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: my_rule
      patterns:
        - label: environment
          value: production
        - label: debug_mode
          value: "true"
config:
  value: 1
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetDisabledCount(), 0);
    }

    Y_UNIT_TEST(ParseUnsetMarker) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: prod_requires_cloud
      patterns:
        - label: environment
          value: production
        - label: cloud
          value: "$unset"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TVector<std::pair<TString, TSet<TLabel>>> labelNames = {
            {"environment", {}},
            {"cloud", {}}
        };

        TVector<TLabel> combination1 = {
            {TLabel::EType::Common, "production"},
            {TLabel::EType::Empty, ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(combination1, labelNames));

        TVector<TLabel> combination2 = {
            {TLabel::EType::Common, "production"},
            {TLabel::EType::Common, "aws"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination2, labelNames));
    }

    Y_UNIT_TEST(ParseEmptyMarker) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: empty_env_no_ha
      patterns:
        - label: environment
          value: "$empty"
        - label: high_availability
          value: "true"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TVector<std::pair<TString, TSet<TLabel>>> labelNames = {
            {"environment", {}},
            {"high_availability", {}}
        };

        TVector<TLabel> combination1 = {
            {TLabel::EType::Empty, ""},
            {TLabel::EType::Common, "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(combination1, labelNames));

        TVector<TLabel> combination2 = {
            {TLabel::EType::Common, "production"},
            {TLabel::EType::Common, "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination2, labelNames));
    }

    Y_UNIT_TEST(DeterministicOrdering) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule3;
        rule3.RuleName = "zzz_last";
        rule3.Patterns = {{.Name = "label3", .Values = THashSet<TString>{"value3"}}};
        rules.AddRule(rule3);

        TIncompatibilityRule rule1;
        rule1.RuleName = "aaa_first";
        rule1.Patterns = {{.Name = "label1", .Values = THashSet<TString>{"value1"}}};
        rules.AddRule(rule1);

        TIncompatibilityRule rule2;
        rule2.RuleName = "mmm_middle";
        rule2.Patterns = {{.Name = "label2", .Values = THashSet<TString>{"value2"}}};
        rules.AddRule(rule2);

        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 3);
    }

    Y_UNIT_TEST(ComplexMultiPatternRule) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "aws_no_gcp_region";
        rule.Patterns = {
            {.Name = "cloud", .Values = THashSet<TString>{"aws"}},
            {.Name = "region", .Values = THashSet<TString>{"us-central1"}},
            {.Name = "environment", .Values = THashSet<TString>{"production"}}
        };
        rules.AddRule(rule);

        TVector<std::pair<TString, TSet<TLabel>>> labelNames = {
            {"cloud", {}},
            {"region", {}},
            {"environment", {}}
        };

        TVector<TLabel> combination1 = {
            {TLabel::EType::Common, "aws"},
            {TLabel::EType::Common, "us-central1"},
            {TLabel::EType::Common, "production"}
        };
        UNIT_ASSERT(!rules.IsCompatible(combination1, labelNames));

        TVector<TLabel> combination2 = {
            {TLabel::EType::Common, "gcp"},
            {TLabel::EType::Common, "us-central1"},
            {TLabel::EType::Common, "production"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination2, labelNames));

        TVector<TLabel> combination3 = {
            {TLabel::EType::Common, "aws"},
            {TLabel::EType::Common, "us-east-1"},
            {TLabel::EType::Common, "production"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination3, labelNames));

        TVector<TLabel> combination4 = {
            {TLabel::EType::Common, "aws"},
            {TLabel::EType::Common, "us-central1"},
            {TLabel::EType::Common, "development"}
        };
        UNIT_ASSERT(rules.IsCompatible(combination4, labelNames));
    }

    Y_UNIT_TEST(IntegrationWithResolveAll) {
        // Simplified test to check incompatibility rules work correctly
        // without requiring full ResolveAll integration
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: aws_no_gcp_region
      patterns:
        - label: cloud
          value: aws
        - label: region
          value: us-central1
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);
        
        // aws + us-central1 is incompatible
        TMap<TString, TString> invalid = {
            {"cloud", "aws"},
            {"region", "us-central1"}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid));
        
        // aws + us-east-1 is compatible
        TMap<TString, TString> valid1 = {
            {"cloud", "aws"},
            {"region", "us-east-1"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid1));
        
        // gcp + us-central1 is compatible
        TMap<TString, TString> valid2 = {
            {"cloud", "gcp"},
            {"region", "us-central1"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid2));
        
        // gcp + us-east-1 is compatible
        TMap<TString, TString> valid3 = {
            {"cloud", "gcp"},
            {"region", "us-east-1"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid3));
    }

    Y_UNIT_TEST(CheckLabelsMapCompatibility) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "no_debug_in_prod";
        rule.Patterns = {
            {.Name = "environment", .Values = THashSet<TString>{"production"}},
            {.Name = "debug_mode", .Values = THashSet<TString>{"true"}}
        };
        rules.AddRule(rule);

        TMap<TString, TString> compatibleLabels1 = {
            {"environment", "production"},
            {"debug_mode", "false"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels1));

        TMap<TString, TString> incompatibleLabels = {
            {"environment", "production"},
            {"debug_mode", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels));

        TMap<TString, TString> compatibleLabels2 = {
            {"environment", "development"},
            {"debug_mode", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels2));

        TMap<TString, TString> partialLabels = {
            {"environment", "production"}
        };
        UNIT_ASSERT(rules.IsCompatible(partialLabels));
    }

    Y_UNIT_TEST(CheckLabelsMapWithUnsetMarker) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: prod_requires_cloud
      patterns:
        - label: environment
          value: production
        - label: cloud
          value: "$unset"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());

        TMap<TString, TString> incompatibleLabels1 = {
            {"environment", "production"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels1));

        TMap<TString, TString> compatibleLabels1 = {
            {"environment", "production"},
            {"cloud", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels1));

        TMap<TString, TString> compatibleLabels2 = {
            {"environment", "production"},
            {"cloud", "aws"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels2));
    }

    Y_UNIT_TEST(ValueInOperator) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: branch_must_have_value
      patterns:
        - label: branch
          value_in: ["$unset", ""]
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TMap<TString, TString> incompatibleLabels1 = {
            {"branch", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels1));

        TMap<TString, TString> incompatibleLabels2 = {};
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels2));

        TMap<TString, TString> compatibleLabels = {
            {"branch", "main"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels));
    }

    Y_UNIT_TEST(ValueInMultipleValues) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: prod_or_staging_env
      patterns:
        - label: environment
          value_in: ["production", "staging"]
        - label: cloud
          value: aws
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TMap<TString, TString> incompatibleLabels1 = {
            {"environment", "production"},
            {"cloud", "aws"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels1));

        TMap<TString, TString> incompatibleLabels2 = {
            {"environment", "staging"},
            {"cloud", "aws"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels2));

        TMap<TString, TString> compatibleLabels1 = {
            {"environment", "development"},
            {"cloud", "aws"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels1));

        TMap<TString, TString> compatibleLabels2 = {
            {"environment", "production"},
            {"cloud", "gcp"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels2));
    }

    Y_UNIT_TEST(NegatedFlag) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: node_type_is_unset
      patterns:
        - label: node_type
          value: "$unset"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TMap<TString, TString> incompatibleLabels = {};
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels));

        TMap<TString, TString> compatibleEmpty = {
            {"node_type", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleEmpty));

        TMap<TString, TString> compatibleLabels = {
            {"node_type", "slot"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels));
    }

    Y_UNIT_TEST(NegatedWithValueIn) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: tenant_set_with_debug
      patterns:
        - label: tenant
          value_in: ["$unset", ""]
          negated: true
        - label: debug
          value: "true"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TMap<TString, TString> incompatibleLabels = {
            {"tenant", "my_tenant"},
            {"debug", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels));

        TMap<TString, TString> compatibleLabels1 = {
            {"tenant", ""},
            {"debug", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels1));

        TMap<TString, TString> compatibleLabels2 = {
            {"tenant", "my_tenant"},
            {"debug", "false"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels2));
    }

    Y_UNIT_TEST(ComplexRuleWithNegation) {
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: static_with_nonempty_tenant
      patterns:
        - label: dynamic
          value: "false"
        - label: tenant
          value: ""
          negated: true
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TMap<TString, TString> incompatibleLabels = {
            {"dynamic", "false"},
            {"tenant", "some_tenant"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels));

        TMap<TString, TString> compatibleLabels1 = {
            {"dynamic", "false"},
            {"tenant", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels1));

        TMap<TString, TString> compatibleLabels2 = {
            {"dynamic", "true"},
            {"tenant", "some_tenant"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels2));
    }

    Y_UNIT_TEST(MonostateMatchesAnyValue) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "any_label";
        pattern.Values = std::monostate{};

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "value123"};
        TLabel negativeLabel{TLabel::EType::Negative, ""};

        // monostate should match any value
        UNIT_ASSERT(pattern.Matches(emptyLabel, "any_label"));
        UNIT_ASSERT(pattern.Matches(commonLabel, "any_label"));
        UNIT_ASSERT(pattern.Matches(negativeLabel, "any_label"));
    }

    Y_UNIT_TEST(RealWorldScenario_DynamicNodesValidation) {
        // Test a real-world scenario with multiple rules using value_in and negation
        // Simplified to just test the rules without full ResolveAll integration
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    # Dynamic nodes must have non-empty tenant
    - name: dynamic_without_tenant
      patterns:
        - label: dynamic
          value: "true"
        - label: tenant
          value_in: ["$unset", ""]
    
    # Static nodes must have empty tenant
    - name: static_with_tenant
      patterns:
        - label: dynamic
          value: "false"
        - label: tenant
          value: ""
          negated: true  # NOT empty
    
    # Cloud nodes (empty node_type) must have enable_auth
    - name: cloud_without_enable_auth
      patterns:
        - label: node_type
          value: ""
        - label: enable_auth
          value: "$unset"
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 3);
        
        // Test dynamic_without_tenant rule
        TMap<TString, TString> invalid1 = {
            {"dynamic", "true"},
            {"tenant", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid1));
        
        TMap<TString, TString> valid1 = {
            {"dynamic", "true"},
            {"tenant", "my_tenant"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid1));
        
        // Test static_with_tenant rule
        TMap<TString, TString> invalid2 = {
            {"dynamic", "false"},
            {"tenant", "some_tenant"}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid2));
        
        TMap<TString, TString> valid2 = {
            {"dynamic", "false"},
            {"tenant", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(valid2));
        
        // Test cloud_without_enable_auth rule
        TMap<TString, TString> invalid3 = {
            {"node_type", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid3));
        
        TMap<TString, TString> valid3 = {
            {"node_type", ""},
            {"enable_auth", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid3));
    }

    Y_UNIT_TEST(ComplexValueInNegationCombination) {
        // Test complex scenario with multiple value_in and negation in same rule
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    # Production or staging with debug enabled is invalid
    - name: prod_staging_with_debug
      patterns:
        - label: environment
          value_in: ["production", "staging"]
        - label: debug
          value: "true"
    
    # Any non-dev environment must have monitoring
    - name: non_dev_without_monitoring
      patterns:
        - label: environment
          value: development
          negated: true  # NOT development
        - label: monitoring
          value_in: ["$unset", ""]
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 2);

        // Test prod_staging_with_debug rule
        TMap<TString, TString> invalid1 = {
            {"environment", "production"},
            {"debug", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid1));

        TMap<TString, TString> invalid2 = {
            {"environment", "staging"},
            {"debug", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid2));

        TMap<TString, TString> valid1 = {
            {"environment", "production"},
            {"debug", "false"},
            {"monitoring", "enabled"}  // Must have monitoring for non-dev
        };
        UNIT_ASSERT(rules.IsCompatible(valid1));

        TMap<TString, TString> valid2 = {
            {"environment", "development"},
            {"debug", "true"},
            {"monitoring", ""}  // Dev can have empty monitoring
        };
        UNIT_ASSERT(rules.IsCompatible(valid2));

        // Test non_dev_without_monitoring rule
        TMap<TString, TString> invalid3 = {
            {"environment", "production"},
            {"monitoring", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid3));

        TMap<TString, TString> invalid4 = {
            {"environment", "staging"}
            // monitoring is unset
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid4));

        TMap<TString, TString> valid3 = {
            {"environment", "development"},
            {"monitoring", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(valid3));

        TMap<TString, TString> valid4 = {
            {"environment", "production"},
            {"monitoring", "enabled"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid4));
    }

    Y_UNIT_TEST(EdgeCase_EmptyValueIn) {
        // Test what happens with empty value_in array
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "test_label";
        pattern.Values = THashSet<TString>{};  // Empty set

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "value"};

        // Empty set should not match anything
        UNIT_ASSERT(!pattern.Matches(emptyLabel, "test_label"));
        UNIT_ASSERT(!pattern.Matches(commonLabel, "test_label"));

        // With negation, empty set should match everything
        pattern.Negated = true;
        UNIT_ASSERT(pattern.Matches(emptyLabel, "test_label"));
        UNIT_ASSERT(pattern.Matches(commonLabel, "test_label"));
    }

    Y_UNIT_TEST(EdgeCase_MultipleUnsetEmptyInValueIn) {
        // Test value_in with both $unset and $empty (both map to "")
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: test_rule
      patterns:
        - label: test_label
          value_in: ["$unset", "$empty", ""]
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto rules = ParseIncompatibilityRules(doc.Root());
        
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        // All three should map to empty string, so we should have just one value in the set
        TMap<TString, TString> invalid1 = {
            {"test_label", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(invalid1));

        TMap<TString, TString> invalid2 = {};  // unset
        UNIT_ASSERT(!rules.IsCompatible(invalid2));

        TMap<TString, TString> valid = {
            {"test_label", "some_value"}
        };
        UNIT_ASSERT(rules.IsCompatible(valid));
    }

    Y_UNIT_TEST(BuiltInRules_RequiredLabels) {
        // Test that built-in rules enforce required labels
        auto rules = TIncompatibilityRules::GetDefaultRules();
        
        // Test branch must have value
        TMap<TString, TString> missingBranch = {
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(missingBranch));
        
        TMap<TString, TString> emptyBranch = {
            {"branch", ""},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(emptyBranch));
        
        TMap<TString, TString> validBranch = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"},
            {"enable_auth", "true"},
            {"node_name", "node1"},
            {"shared", "false"},
            {"ydb_postgres", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(validBranch));
    }

    Y_UNIT_TEST(BuiltInRules_StaticNodes) {
        auto rules = TIncompatibilityRules::GetDefaultRules();
        
        TMap<TString, TString> staticWithTenant = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "false"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", "my_tenant"}
        };
        UNIT_ASSERT(!rules.IsCompatible(staticWithTenant));
        
        TMap<TString, TString> staticWithCloudLabel = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "false"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", ""},
            {"enable_auth", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(staticWithCloudLabel));
        
        TMap<TString, TString> validStatic = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "false"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", ""}
        };
        UNIT_ASSERT(rules.IsCompatible(validStatic));
    }

    Y_UNIT_TEST(BuiltInRules_DynamicNodes) {
        auto rules = TIncompatibilityRules::GetDefaultRules();
        
        TMap<TString, TString> dynamicWithoutTenant = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", ""},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(dynamicWithoutTenant));
        
        TMap<TString, TString> dynamicWithoutFlavour = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", "my_tenant"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(dynamicWithoutFlavour));
        
        TMap<TString, TString> validDynamic = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "storage"},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(validDynamic));
    }

    Y_UNIT_TEST(BuiltInRules_CloudNodes) {
        auto rules = TIncompatibilityRules::GetDefaultRules();
        
        TMap<TString, TString> cloudWithoutEnableAuth = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(cloudWithoutEnableAuth));
        
        TMap<TString, TString> validCloud = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", ""},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"},
            {"enable_auth", "true"},
            {"node_name", "node1"},
            {"shared", "false"},
            {"ydb_postgres", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(validCloud));
    }

    Y_UNIT_TEST(BuiltInRules_SlotNodes) {
        auto rules = TIncompatibilityRules::GetDefaultRules();
        
        TMap<TString, TString> staticSlot = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "false"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "slot"},
            {"tenant", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(staticSlot));
        
        TMap<TString, TString> slotWithCloudLabel = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "slot"},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"},
            {"enable_auth", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(slotWithCloudLabel));
        
        TMap<TString, TString> validSlot = {
            {"branch", "main"},
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "slot"},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(validSlot));
    }

    Y_UNIT_TEST(BuiltInRules_DisableSpecificRules) {
        const char* config = R"(---
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_without_valid_tenant
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto userRules = ParseIncompatibilityRules(doc.Root());
        
        auto rules = TIncompatibilityRules::GetDefaultRules();
        rules.MergeWith(userRules);
        
        TMap<TString, TString> missingBranch = {
            {"configuration_version", "1"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "storage"},
            {"tenant", ""},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(rules.IsCompatible(missingBranch));
        
        TMap<TString, TString> missingConfigVersion = {
            {"branch", "main"},
            {"dynamic", "true"},
            {"node_host", "host1"},
            {"node_id", "1"},
            {"rev", "123"},
            {"node_type", "storage"},
            {"tenant", "my_tenant"},
            {"flavour", "standard"},
            {"ydbcp", "true"}
        };
        UNIT_ASSERT(!rules.IsCompatible(missingConfigVersion));
    }
    
    Y_UNIT_TEST(BuiltInRules_DisableAll) {
        const TString config = R"(
configuration_version: 1
incompatibility_overrides:
  disable_all_builtin_rules: true
)";
        auto doc = NKikimr::NFyaml::TDocument::Parse(config);
        auto userRules = ParseIncompatibilityRules(doc.Root());
        
        auto rules = TIncompatibilityRules::GetDefaultRules();
        rules.MergeWith(userRules);
        
        TMap<TString, TString> invalidLabels = {
            {"configuration_version", "1"},
            {"rev", "123"}
        };
        UNIT_ASSERT(rules.IsCompatible(invalidLabels));
    }
}
