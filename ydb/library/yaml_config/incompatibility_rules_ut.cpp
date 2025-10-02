#include "public/yaml_config.h"
#include "public/yaml_config_impl.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NYamlConfig;

Y_UNIT_TEST_SUITE(IncompatibilityRules) {
    Y_UNIT_TEST(BasicPatternMatching) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "environment";
        pattern.Value = TString("production");

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
        pattern.Value = TString("");

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "some_value"};

        UNIT_ASSERT(pattern.Matches(emptyLabel, "tenant"));
        UNIT_ASSERT(!pattern.Matches(commonLabel, "tenant"));
    }

    Y_UNIT_TEST(UnsetLabelMatching) {
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = "cloud";
        pattern.Value = std::monostate{};

        TLabel emptyLabel{TLabel::EType::Empty, ""};
        TLabel commonLabel{TLabel::EType::Common, "aws"};
        TLabel negativeLabel{TLabel::EType::Negative, ""};

        UNIT_ASSERT(pattern.Matches(emptyLabel, "cloud"));
        UNIT_ASSERT(!pattern.Matches(commonLabel, "cloud"));
        UNIT_ASSERT(!pattern.Matches(negativeLabel, "cloud"));
    }

    Y_UNIT_TEST(AddAndRemoveRules) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule1;
        rule1.RuleName = "test_rule_1";
        rule1.Patterns = {
            {.Name = "env", .Value = TString("prod")}
        };
        
        TIncompatibilityRule rule2;
        rule2.RuleName = "test_rule_2";
        rule2.Patterns = {
            {.Name = "cloud", .Value = TString("aws")}
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
            {.Name = "env", .Value = TString("prod")}
        };
        
        rules.AddRule(rule1);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);

        TIncompatibilityRule rule2;
        rule2.RuleName = "test_rule";
        rule2.Patterns = {
            {.Name = "env", .Value = TString("dev")}
        };
        
        rules.AddRule(rule2);
        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 1);
    }

    Y_UNIT_TEST(SimpleIncompatibilityCheck) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "no_debug_in_prod";
        rule.Patterns = {
            {.Name = "environment", .Value = TString("production")},
            {.Name = "debug_mode", .Value = TString("true")}
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
            {.Name = "env", .Value = TString("prod")},
            {.Name = "debug", .Value = TString("true")}
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

        TIncompatibilityRules userRules;
        userRules.DisabledRules.insert("test_rule");
        rules.MergeWith(userRules);

        UNIT_ASSERT(rules.IsCompatible(combination, labelNames));
        UNIT_ASSERT_VALUES_EQUAL(rules.GetDisabledCount(), 1);
    }

    Y_UNIT_TEST(MergeRules) {
        TIncompatibilityRules baseRules;
        
        TIncompatibilityRule rule1;
        rule1.RuleName = "base_rule";
        rule1.Patterns = {
            {.Name = "env", .Value = TString("prod")}
        };
        baseRules.AddRule(rule1);

        TIncompatibilityRules userRules;
        
        TIncompatibilityRule rule2;
        rule2.RuleName = "user_rule";
        rule2.Patterns = {
            {.Name = "cloud", .Value = TString("aws")}
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
        auto doc = NFyaml::TDocument::Parse(config);
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
        auto doc = NFyaml::TDocument::Parse(config);
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
        auto doc = NFyaml::TDocument::Parse(config);
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
        auto doc = NFyaml::TDocument::Parse(config);
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
        auto doc = NFyaml::TDocument::Parse(config);
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
        rule3.Patterns = {{.Name = "label3", .Value = TString("value3")}};
        rules.AddRule(rule3);

        TIncompatibilityRule rule1;
        rule1.RuleName = "aaa_first";
        rule1.Patterns = {{.Name = "label1", .Value = TString("value1")}};
        rules.AddRule(rule1);

        TIncompatibilityRule rule2;
        rule2.RuleName = "mmm_middle";
        rule2.Patterns = {{.Name = "label2", .Value = TString("value2")}};
        rules.AddRule(rule2);

        UNIT_ASSERT_VALUES_EQUAL(rules.GetRuleCount(), 3);
    }

    Y_UNIT_TEST(ComplexMultiPatternRule) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "aws_no_gcp_region";
        rule.Patterns = {
            {.Name = "cloud", .Value = TString("aws")},
            {.Name = "region", .Value = TString("us-central1")},
            {.Name = "environment", .Value = TString("production")}
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
        const char* config = R"(---
incompatibility_overrides:
  custom_rules:
    - name: aws_no_gcp_region
      patterns:
        - label: cloud
          value: aws
        - label: region
          value: us-central1

selector_config:
  - description: AWS US-East Config
    selector:
      cloud: aws
      region: us-east-1
    config:
      value: aws_east
  - description: AWS Central Config
    selector:
      cloud: aws
      region: us-central1
    config:
      value: aws_central
  - description: GCP US-East Config
    selector:
      cloud: gcp
      region: us-east-1
    config:
      value: gcp_east
  - description: GCP Central Config
    selector:
      cloud: gcp
      region: us-central1
    config:
      value: gcp_central

config:
  base_value: 1
)";
        auto doc = NFyaml::TDocument::Parse(config);
        auto resolved = ResolveAll(doc);
        
        UNIT_ASSERT(resolved.Configs.size() < 4);
    }

    Y_UNIT_TEST(CheckLabelsMapCompatibility) {
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "no_debug_in_prod";
        rule.Patterns = {
            {.Name = "environment", .Value = TString("production")},
            {.Name = "debug_mode", .Value = TString("true")}
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
        TIncompatibilityRules rules;
        
        TIncompatibilityRule rule;
        rule.RuleName = "prod_requires_cloud";
        rule.Patterns = {
            {.Name = "environment", .Value = TString("production")},
            {.Name = "cloud", .Value = std::monostate{}}
        };
        rules.AddRule(rule);

        TMap<TString, TString> incompatibleLabels1 = {
            {"environment", "production"}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels1));

        TMap<TString, TString> incompatibleLabels2 = {
            {"environment", "production"},
            {"cloud", ""}
        };
        UNIT_ASSERT(!rules.IsCompatible(incompatibleLabels2));

        TMap<TString, TString> compatibleLabels = {
            {"environment", "production"},
            {"cloud", "aws"}
        };
        UNIT_ASSERT(rules.IsCompatible(compatibleLabels));
    }
}
