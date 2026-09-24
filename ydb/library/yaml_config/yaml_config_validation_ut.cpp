#include "yaml_config.h"

#include <ydb/core/protos/auth.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

#include <stdexcept>

using namespace NKikimr;
using namespace NKikimr::NYamlConfig;

namespace {

TString ValidationError(NFyaml::TDocument& doc, const IConfigSwissKnife* validator, bool legacy,
    TSimpleSharedPtr<NProtobufJson::IUnknownFieldsCollector> collector = nullptr)
{
    try {
        if (legacy) {
            std::vector<TString> errors;
            ResolveUniqueDocs(doc, [&](TDocumentConfig&& config) {
                const auto proto = YamlToProto(config.second, true, true, collector);
                if (validator && validator->ValidateConfig(proto, errors) == EValidationResult::Error) {
                    ythrow yexception() << errors.front();
                }
            });
        } else {
            ValidateConfig(doc, validator, collector);
        }
        return {};
    } catch (const std::exception& e) {
        return e.what();
    }
}

TString CheckValidation(NFyaml::TDocument& doc, bool rejected, const IConfigSwissKnife* validator) {
    const auto legacy = ValidationError(doc, validator, true);
    const auto projected = ValidationError(doc, validator, false);
    UNIT_ASSERT_VALUES_EQUAL_C(!legacy.empty(), rejected, legacy);
    UNIT_ASSERT_VALUES_EQUAL_C(!projected.empty(), rejected, projected);
    return projected;
}

void CheckValidation(const TString& yaml, bool rejected) {
    auto doc = NFyaml::TDocument::Parse(yaml);
    const auto validator = CreateDefaultConfigSwissKnife();
    CheckValidation(doc, rejected, validator.get());
}

TString ProjectionKey(NFyaml::TNodeRef config, const TVector<TString>& sections) {
    TStringStream result;
    for (const auto& section : sections) {
        result << section << "=";
        const TString key = section.substr(1);
        if (config.Map().Has(key)) {
            result << config.Map().at(key);
        }
        result << "\n";
    }
    return result.Str();
}

TString SharedInputConfig(const TString& selectors, bool enforceTokens = true) {
    return TStringBuilder() << R"(
config:
  domains_config:
    domain: [{name: sample}]
    state_storage: [{ssid: 1, ring: {node: [1], nto_select: 1}}]
    security_config:
      enforce_user_token_requirement: )" << (enforceTokens ? "true" : "false") << R"(
  monitoring_config: {require_counters_authentication: false}
allowed_labels:
  deployment: {type: string}
  test: {type: string}
selector_config:
)" << selectors;
}

} // namespace

Y_UNIT_TEST_SUITE(YamlConfigValidation) {
    Y_UNIT_TEST(ConfigV2AndGrpcPreserveCorrelations) {
        for (bool correlated : {false, true}) {
            CheckValidation(TStringBuilder() << R"(
config:
  feature_flags: {switch_to_config_v2: false}
  grpc_config: {port: 2135, start_grpc_proxy: true, services_disabled: [config]}
allowed_labels:
  deployment: {type: string}
  test: {type: string}
selector_config:
- description: enable config v2
  selector: {deployment: selected}
  config:
    feature_flags: {switch_to_config_v2: true}
- description: enable the required service
  selector: {)" << (correlated ? "deployment" : "test") << R"(: selected}
  config:
    grpc_config: !inherit
      services_disabled: []
)", !correlated);
        }
    }

    Y_UNIT_TEST(CoupledNbsGrpc) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  nbs_config:
    enabled: true
    nbs_frontend_config:
      enabled: false
  grpc_config:
    port: 2135
    start_grpc_proxy: true
allowed_labels:
  a: {type: string}
  b: {type: string}
selector_config:
- description: enable frontend
  selector: {a: on}
  config:
    nbs_config: !inherit
      nbs_frontend_config: {enabled: true}
- description: disable proxy
  selector: {b: on}
  config:
    grpc_config: !inherit
      start_grpc_proxy: false
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, true, validator.get());
    }

    Y_UNIT_TEST(UnrealizableBase) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: test}
  auth_config:
    password_complexity:
      min_length: 1
      min_lower_case_count: 2
allowed_labels:
  a: {type: enum, values: {x: {}}}
selector_config:
- description: fix all a values
  selector: {a: ''}
  config:
    auth_config: !inherit
      password_complexity: !inherit
        min_length: 20
- description: fix a=x
  selector: {a: x}
  config:
    auth_config: !inherit
      password_complexity: !inherit
        min_length: 20
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, false, validator.get());
    }

    Y_UNIT_TEST(ExternalAnchor) {
        auto doc = NFyaml::TDocument::Parse(R"(
default_log: &lc
  cluster_name: test
config:
  log_config: *lc
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, false, validator.get());
    }

    Y_UNIT_TEST(EmptyInheritWrongType) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: base}
allowed_labels:
  a: {type: string}
selector_config:
- description: invalid merge
  selector: {a: x}
  config:
    log_config: !inherit
      cluster_name: !inherit {}
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, true, validator.get());
    }

    Y_UNIT_TEST(ClientCertificateAndGrpcAreCheckedJointly) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  grpc_config: {ca: certificate}
  client_certificate_authorization:
    request_client_certificate: true
    client_certificate_required: false
allowed_labels:
  a: {type: string}
  b: {type: string}
selector_config:
- description: require certificate
  selector: {a: x}
  config:
    client_certificate_authorization: !inherit
      client_certificate_required: true
- description: remove CA
  selector: {b: x}
  config:
    grpc_config: {}
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, true, validator.get());
    }

    Y_UNIT_TEST(StructuralChecksIgnoreUnrealizableUnrelatedBase) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: test}
  actor_system_config: {}
allowed_labels:
  a: {type: enum, values: {x: {}}}
selector_config:
- description: cover the entire domain
  selector: {a: {in: ['', x]}}
  config:
    actor_system_config: {use_auto_config: true}
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        CheckValidation(doc, false, validator.get());
    }

    Y_UNIT_TEST(IndependentSelectorsConvergeToSameConfig) {
        auto doc = NFyaml::TDocument::Parse(R"(
config: {log_config: {cluster_name: base}}
allowed_labels:
  deployment: {type: string}
  test: {type: string}
selector_config:
- description: deployment override
  selector: {deployment: selected}
  config: {log_config: {cluster_name: selected}}
- description: test override
  selector: {test: selected}
  config: {log_config: {cluster_name: selected}}
)");
        TSet<TString> names;
        size_t count = 0;
        EnumerateDistinctProjections(doc, {"/log_config"}, [&](NFyaml::TNodeRef config) {
            ++count;
            names.insert(config.Map().at("log_config").Map().at("cluster_name").Scalar());
        });
        UNIT_ASSERT_VALUES_EQUAL(count, 2);
        UNIT_ASSERT(names.contains("base"));
        UNIT_ASSERT(names.contains("selected"));
    }

    Y_UNIT_TEST(SameSectionScaling) {
        TStringBuilder yaml;
        yaml << "config:\n  log_config: {cluster_name: base}\nallowed_labels:\n";
        for (int i = 0; i < 32; ++i) {
            yaml << "  l" << i << ": {type: enum, values: {x: {}}}\n";
        }
        yaml << "selector_config:\n";
        for (int i = 0; i < 32; ++i) {
            yaml << "- description: s\n  selector: {l" << i << ": x}\n"
                 << "  config:\n    log_config: {cluster_name: selected}\n";
        }
        auto doc = NFyaml::TDocument::Parse(yaml);
        size_t count = 0;
        NYamlConfig::EnumerateDistinctProjections(doc, {"/log_config"},
            [&](NFyaml::TNodeRef) { ++count; });
        UNIT_ASSERT_VALUES_EQUAL(count, 2);
    }

    Y_UNIT_TEST(ManyValuesOfOneLabelWithRedundantSelectors) {
        TStringBuilder yaml;
        yaml << "config:\n  log_config: {cluster_name: base}\n"
             << "allowed_labels:\n  deployment: {type: string}\nselector_config:\n";
        for (size_t i = 0; i < 10000; ++i) {
            yaml << "- description: redundant\n  selector: {deployment: v" << i << "}\n"
                 << "  config: {log_config: {cluster_name: base}}\n";
        }
        auto doc = NFyaml::TDocument::Parse(yaml);
        size_t count = 0;
        EnumerateDistinctProjections(doc, {"/log_config"}, [&](NFyaml::TNodeRef config) {
            ++count;
            UNIT_ASSERT_VALUES_EQUAL(config.Map().at("log_config").Map().at("cluster_name").Scalar(), "base");
        });
        UNIT_ASSERT_VALUES_EQUAL(count, 1);
    }

    Y_UNIT_TEST(ManyValuesOfOneLabelConvergeToNonBaseConfig) {
        TStringBuilder yaml;
        yaml << "config:\n  log_config: {cluster_name: base}\n"
             << "allowed_labels:\n  deployment: {type: string}\nselector_config:\n";
        for (size_t i = 0; i < 10000; ++i) {
            yaml << "- description: converging\n  selector: {deployment: v" << i << "}\n"
                 << "  config: {log_config: {cluster_name: selected}}\n";
        }
        auto doc = NFyaml::TDocument::Parse(yaml);
        TSet<TString> names;
        size_t count = 0;
        EnumerateDistinctProjections(doc, {"/log_config"}, [&](NFyaml::TNodeRef config) {
            ++count;
            names.insert(config.Map().at("log_config").Map().at("cluster_name").Scalar());
        });
        UNIT_ASSERT_VALUES_EQUAL(count, 2);
        UNIT_ASSERT(names.contains("base"));
        UNIT_ASSERT(names.contains("selected"));
    }

    Y_UNIT_TEST(BinaryLabelEncodingExcludesPaddingValues) {
        for (size_t size = 0; size < 18; ++size) {
            TStringBuilder yaml;
            yaml << "config: {log_config: {cluster_name: base}}\n"
                 << "allowed_labels:\n  deployment: {type: enum, values: {";
            for (size_t i = 0; i < size; ++i) {
                yaml << (i ? ", " : "") << "v" << i << ": {}";
            }
            yaml << "}}\nselector_config:\n"
                 << "- description: empty label\n  selector: {deployment: ''}\n"
                 << "  config: {log_config: {cluster_name: selected}}\n";
            for (size_t i = 0; i < size; ++i) {
                yaml << "- description: named value\n  selector: {deployment: v" << i << "}\n"
                     << "  config: {log_config: {cluster_name: selected}}\n";
            }
            auto doc = NFyaml::TDocument::Parse(yaml);
            size_t count = 0;
            EnumerateDistinctProjections(doc, {"/log_config"}, [&](NFyaml::TNodeRef config) {
                ++count;
                UNIT_ASSERT_VALUES_EQUAL(config.Map().at("log_config").Map().at("cluster_name").Scalar(), "selected");
            });
            UNIT_ASSERT_VALUES_EQUAL_C(count, 1, size);
        }
    }

    Y_UNIT_TEST(RegionReclamationPreservesCorrelationsAndRules) {
        TStringBuilder yaml;
        yaml << R"(
config: {log_config: {cluster_name: base}, other: base}
allowed_labels:
  deployment: {type: string}
  test: {type: string}
incompatibility_overrides:
  custom_rules:
  - name: forbidden_pair
    patterns:
    - {label: deployment, value: v0}
    - {label: test, value: blocked}
selector_config:
)";
        // Grow and then shrink correlated regions, reclaiming historical prefixes
        // and reusing node IDs before later selectors query those label values.
        for (bool reset : {false, true}) {
            for (size_t i = 0; i < 128; ++i) {
                yaml << "- description: converge\n  selector: {deployment: v" << (reset ? 127 - i : i)
                     << ", test: {not_in: [off]}}\n"
                     << "  config: {log_config: {cluster_name: " << (reset ? "base" : "selected") << "}}\n";
            }
        }
        for (size_t i : {0, 64, 127}) {
            yaml << "- description: query reclaimed region\n  selector: {deployment: v" << i << "}\n"
                 << "  config: {log_config: {cluster_name: value" << i << "}}\n";
        }
        yaml << R"(
- description: observe correlation
  selector: {test: off}
  config: {other: off}
- description: observe compatibility rule
  selector: {deployment: v0, test: blocked}
  config: {log_config: {cluster_name: forbidden}}
)";
        auto doc = NFyaml::TDocument::Parse(yaml);
        const TVector<TString> sections = {"/log_config", "/other"};
        TSet<TString> expected;
        ResolveUniqueDocs(doc, [&](TDocumentConfig&& config) {
            expected.insert(ProjectionKey(config.second, sections));
        });
        TSet<TString> actual;
        size_t count = 0;
        EnumerateDistinctProjections(doc, sections, [&](NFyaml::TNodeRef config) {
            actual.insert(ProjectionKey(config, sections));
            ++count;
        });
        UNIT_ASSERT_VALUES_EQUAL(count, actual.size());
        UNIT_ASSERT_C(actual == expected, "reclaimed regions must preserve legacy results");
    }

    Y_UNIT_TEST(CustomSwissKnifeReceivesCompleteConfigurations) {
        class TSwissKnife : public NYamlConfig::IConfigSwissKnife {
        public:
            bool VerifyReplaceRequest(const Ydb::Config::ReplaceConfigRequest&, Ydb::StatusIds::StatusCode&, NYql::TIssues&) const override {
                return true;
            }
            bool VerifyMainConfig(const TString&) const override { return true; }
            bool VerifyStorageConfig(const TString&) const override { return true; }
            NYamlConfig::EValidationResult ValidateConfig(const NKikimrConfig::TAppConfig& config,
                std::vector<TString>& errors) const override
            {
                if (config.GetLogConfig().GetClusterName() == "selected"
                    && config.GetAuthConfig().GetPasswordComplexity().GetMinLength() == 20) {
                    errors.push_back("custom joint check");
                    return NYamlConfig::EValidationResult::Error;
                }
                return NYamlConfig::EValidationResult::Ok;
            }
        } swissKnife;
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: base}
  auth_config: {password_complexity: {min_length: 10}}
allowed_labels:
  a: {type: string}
  b: {type: string}
selector_config:
- description: log variant
  selector: {a: x}
  config:
    log_config: {cluster_name: selected}
- description: auth variant
  selector: {b: x}
  config:
    auth_config: {password_complexity: {min_length: 20}}
)");
        CheckValidation(doc, false, CreateDefaultConfigSwissKnife().get());
        const auto error = CheckValidation(doc, true, &swissKnife);
        UNIT_ASSERT_C(error.Contains("custom joint check"), error);
    }

    Y_UNIT_TEST(SymbolicMergesMatchLegacyWithCorrelationsAndRules) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config:
    cluster_name: base
    entry: [{component: A, level: 1}]
  other: base
allowed_labels:
  a: {type: string}
  b: {type: enum, values: {x: {}, y: {}, z: {}, w: {}}}
  c: {type: string}
incompatibility_overrides:
  custom_rules:
  - name: forbidden_pair
    patterns:
    - {label: a, value: x}
    - {label: c, value: y}
selector_config:
- description: append
  selector: {a: x}
  config:
    log_config: !inherit
      entry: !append [{component: B, level: 2}]
- description: correlated deep merge
  selector: {a: x, b: {not_in: [y]}}
  config:
    log_config: !inherit
      cluster_name: selected
      entry: !inherit:component
      - !inherit {component: A, level: 3}
- description: delete keyed entry
  selector: {b: y}
  config:
    log_config: !inherit
      entry: !inherit:component
      - !remove {component: A}
- description: replace
  selector: {a: {not_in: [x]}, b: x}
  config:
    log_config: {cluster_name: replaced}
- description: match most values of a closed domain
  selector: {b: {in: [x, y, z, w]}}
  config: {other: majority}
- description: match the complementary minority
  selector: {b: {not_in: [x, y, z, w]}}
  config: {other: minority}
- description: rule-only outside dependency
  selector: {c: y}
  config: {other: selected}
)");
        for (const TVector<TString>& sections : {TVector<TString>{"/log_config"}, TVector<TString>{"/other"},
                TVector<TString>{"/log_config", "/other"}}) {
            TSet<TString> expected;
            NYamlConfig::ResolveUniqueDocs(doc, [&](NYamlConfig::TDocumentConfig&& cfg) {
                expected.insert(ProjectionKey(cfg.second, sections));
            });
            TSet<TString> actual;
            size_t count = 0;
            NYamlConfig::EnumerateDistinctProjections(doc, sections, [&](NFyaml::TNodeRef config) {
                actual.insert(ProjectionKey(config, sections));
                ++count;
            });
            UNIT_ASSERT_VALUES_EQUAL(count, actual.size());
            UNIT_ASSERT_C(actual == expected, "projection content must equal legacy enumeration");
        }
    }

    Y_UNIT_TEST(RedundantAndChangingWritesAreAccepted) {
        for (bool value : {false, true}) {
            CheckValidation(SharedInputConfig(TStringBuilder() << R"(
- description: shared input change or redundant write
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      security_config: !inherit
        enforce_user_token_requirement: )" << (value ? "true" : "false") << "\n"), false);
        }
    }

    Y_UNIT_TEST(SharedAndIndependentInputsPreserveCorrelations) {
        for (bool correlated : {false, true}) {
            CheckValidation(SharedInputConfig(TStringBuilder() << R"(
- description: require authentication
  selector: {deployment: selected}
  config:
    monitoring_config: {require_counters_authentication: true}
- description: enable token enforcement
  selector: {)" << (correlated ? "deployment" : "test") << R"(: selected}
  config:
    domains_config: !inherit
      security_config: !inherit
        enforce_user_token_requirement: true
)", false), !correlated);
        }
    }

    Y_UNIT_TEST(EphemeralSecurityInputIsResolvedWithMonitoring) {
        for (bool correlated : {false, true}) {
            CheckValidation(SharedInputConfig(TStringBuilder() << R"(
- description: require authentication
  selector: {deployment: selected}
  config:
    monitoring_config: {require_counters_authentication: true}
- description: security shorthand is a shared transform input
  selector: {)" << (correlated ? "deployment" : "test") << R"(: selected}
  config:
    security_config: {enforce_user_token_requirement: true}
)", false), !correlated);
        }
    }

    Y_UNIT_TEST(SharedInputReplacementPreservesSemanticChecks) {
        for (bool requireAuth : {false, true}) {
            CheckValidation(SharedInputConfig(TStringBuilder() << R"(
- description: replace security mapping and remove token requirement
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      security_config: {}
    monitoring_config: {require_counters_authentication: )" << (requireAuth ? "true" : "false") << "}\n"), requireAuth);
        }
    }

    Y_UNIT_TEST(SharedInputTransformErrorsAreRejected) {
        CheckValidation(SharedInputConfig(R"(
- description: only domain id one is supported by the transform
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      domain: [{name: sample, domain_id: 2}]
)"), true);
    }

    Y_UNIT_TEST(UnreachableSharedInputErrorsAreIgnored) {
        const TString yaml = SharedInputConfig(R"(
- description: unreachable malformed domain
  selector: {deployment: selected, test: selected}
  config:
    domains_config: !inherit
      domain: [{name: sample, domain_id: 2}]
)") + R"(
incompatibility_overrides:
  custom_rules:
  - name: exclude_invalid_pair
    patterns:
    - {label: deployment, value: selected}
    - {label: test, value: selected}
)";
        CheckValidation(yaml, false);
    }

    Y_UNIT_TEST(OverwrittenSharedInputErrorsAreIgnored) {
        CheckValidation(SharedInputConfig(R"(
- description: intermediate invalid value
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      domain: [{name: sample, domain_id: 2}]
- description: repair the same realizable branch before validation
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      domain: [{name: sample, domain_id: 1}]
)"), false);
    }

    Y_UNIT_TEST(InvalidSharedBaseRepairedForAllLabelsIsAccepted) {
        CheckValidation(R"(
config:
  domains_config:
    domain: [{name: sample, domain_id: 2}]
    state_storage: [{ssid: 1, ring: {node: [1], nto_select: 1}}]
  feature_flags: {enable_background_compaction: true}
allowed_labels:
  deployment: {type: string}
selector_config:
- description: unconditional repair
  selector: {}
  config:
    domains_config: !inherit
      domain: [{name: sample, domain_id: 1}]
- description: unrelated variation
  selector: {deployment: selected}
  config:
    feature_flags: !inherit
      enable_background_compaction: false
)", false);
    }

    Y_UNIT_TEST(StateStorageAndSelfManagementVaryJointly) {
        for (bool correlated : {false, true}) {
            CheckValidation(TStringBuilder() << R"(
config:
  domains_config:
    domain: [{name: sample}]
    state_storage: [{ssid: 1, ring: {node: [1], nto_select: 1}}]
  self_management_config: {enabled: false}
  default_disk_type: SSD
allowed_labels:
  deployment: {type: string}
  test: {type: string}
selector_config:
- description: legacy state storage is invalid when self-management is off
  selector: {deployment: selected}
  config:
    domains_config: !inherit
      state_storage: [{ssid: 1, ring: {node: [1], nto_select: 2}}]
- description: self-management supersedes legacy state storage
  selector: {)" << (correlated ? "deployment" : "test") << R"(: selected}
  config:
    self_management_config: {enabled: true, erasure_species: none}
)", !correlated);
        }
    }

    Y_UNIT_TEST(SharedSectionCanBecomeAbsent) {
        CheckValidation(SharedInputConfig(R"(
- description: remove the shared section entirely
  selector: {deployment: selected}
  config:
    domains_config: null
)"), false);
    }

}

Y_UNIT_TEST_SUITE(YamlConfigValidationCompatibility) {
    Y_UNIT_TEST(NullValidatorOnlyChecksConversion) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  auth_config:
    password_complexity: {min_length: 1, min_lower_case_count: 2}
)");
        CheckValidation(doc, false, nullptr);
        CheckValidation(doc, true, CreateDefaultConfigSwissKnife().get());
    }

    Y_UNIT_TEST(ResolvedFieldDiagnosticsMatchLegacy) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: base}
allowed_labels:
  deployment: {type: string}
  test: {type: string}
selector_config:
- description: overwritten unknown field
  selector: {deployment: selected}
  config:
    log_config: {future_log_field: value}
- description: replacement removes the unknown field
  selector: {deployment: selected}
  config:
    log_config: {cluster_name: selected}
- description: retained unknown field
  selector: {test: selected}
  config:
    feature_flags: {future_feature: true}
)");
        const auto validator = CreateDefaultConfigSwissKnife();
        auto legacy = MakeSimpleShared<TBasicUnknownFieldsCollector>();
        auto projected = MakeSimpleShared<TBasicUnknownFieldsCollector>();
        UNIT_ASSERT(ValidationError(doc, validator.get(), true, legacy).empty());
        UNIT_ASSERT(ValidationError(doc, validator.get(), false, projected).empty());
        UNIT_ASSERT_VALUES_EQUAL(legacy->GetUnknownKeys().size(), 1);
        UNIT_ASSERT(projected->GetUnknownKeys() == legacy->GetUnknownKeys());
    }

    Y_UNIT_TEST(EquivalentMappingOrdersAreMerged) {
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  feature_flags: {enable_background_compaction: true, enable_not_null_columns: true}
allowed_labels:
  deployment: {type: string}
selector_config:
- description: changes mapping order only
  selector: {deployment: selected}
  config:
    feature_flags: !inherit
      enable_background_compaction: true
)");
        size_t count = 0;
        EnumerateDistinctProjections(doc, {"/feature_flags"}, [&](NFyaml::TNodeRef) { ++count; });
        UNIT_ASSERT_VALUES_EQUAL(count, 1);
        CheckValidation(doc, false, CreateDefaultConfigSwissKnife().get());
    }

    Y_UNIT_TEST(QuotedNullRemainsAString) {
        auto doc = NFyaml::TDocument::Parse(R"(
config: {config_dir_path: null}
allowed_labels:
  deployment: {type: string}
selector_config:
- description: string value with null spelling
  selector: {deployment: selected}
  config: {config_dir_path: "null"}
)");
        TSet<bool> presence;
        EnumerateDistinctProjections(doc, {"/config_dir_path"}, [&](NFyaml::TNodeRef config) {
            presence.insert(YamlToProto(config).HasConfigDirPath());
        });
        UNIT_ASSERT_VALUES_EQUAL(presence.size(), 2);
        CheckValidation(doc, false, CreateDefaultConfigSwissKnife().get());
    }

    Y_UNIT_TEST(ValidationErrorsDoNotRetryFullResolution) {
        class TValidator : public IConfigSwissKnife {
        public:
            bool VerifyReplaceRequest(const Ydb::Config::ReplaceConfigRequest&, Ydb::StatusIds::StatusCode&, NYql::TIssues&) const override { return true; }
            bool VerifyMainConfig(const TString&) const override { return true; }
            bool VerifyStorageConfig(const TString&) const override { return true; }
            TVector<TVector<TString>> GetValidationDependencies() const override {
                return {{"/auth_config"}, {"/log_config"}};
            }
            EValidationResult ValidateConfig(const NKikimrConfig::TAppConfig& config, std::vector<TString>& errors) const override {
                ++Calls;
                if (ThrowUnexpected) {
                    throw std::runtime_error("unexpected validator failure");
                }
                if (config.GetLogConfig().GetClusterName() == "invalid") {
                    errors.push_back("logging failure");
                } else if (config.GetAuthConfig().GetPasswordComplexity().GetMinLength() == 1) {
                    errors.push_back("authentication failure");
                } else {
                    return EValidationResult::Ok;
                }
                return EValidationResult::Error;
            }
            mutable size_t Calls = 0;
            bool ThrowUnexpected = false;
        } validator;
        auto doc = NFyaml::TDocument::Parse(R"(
config:
  log_config: {cluster_name: invalid}
  auth_config: {password_complexity: {min_length: 1}}
)");
        UNIT_ASSERT(ValidationError(doc, &validator, true).Contains("logging failure"));
        validator.Calls = 0;
        UNIT_ASSERT(ValidationError(doc, &validator, false).Contains("authentication failure"));
        UNIT_ASSERT_VALUES_EQUAL(validator.Calls, 1);

        validator.ThrowUnexpected = true;
        validator.Calls = 0;
        UNIT_ASSERT(ValidationError(doc, &validator, false).Contains("unexpected validator failure"));
        UNIT_ASSERT_VALUES_EQUAL(validator.Calls, 1);
    }

    Y_UNIT_TEST(IndependentSectionsHaveAdditiveValidationWork) {
        class TCounter : public IConfigSwissKnife {
        public:
            bool VerifyReplaceRequest(const Ydb::Config::ReplaceConfigRequest&, Ydb::StatusIds::StatusCode&, NYql::TIssues&) const override { return true; }
            bool VerifyMainConfig(const TString&) const override { return true; }
            bool VerifyStorageConfig(const TString&) const override { return true; }
            TVector<TVector<TString>> GetValidationDependencies() const override { return {}; }
            EValidationResult ValidateConfig(const NKikimrConfig::TAppConfig&, std::vector<TString>&) const override {
                ++Calls;
                return EValidationResult::Ok;
            }
            mutable size_t Calls = 0;
        } counter;
        TStringBuilder yaml;
        yaml << "config:\n  log_config: {default_level: 0}\n"
             << "  auth_config: {password_complexity: {min_length: 20}}\n"
             << "allowed_labels:\n  deployment: {type: string}\n  test: {type: string}\nselector_config:\n";
        constexpr size_t variants = 20;
        for (size_t i = 1; i <= variants; ++i) {
            yaml << "- description: logging\n  selector: {deployment: v" << i << "}\n"
                 << "  config: {log_config: {default_level: " << i << "}}\n"
                 << "- description: authentication\n  selector: {test: v" << i << "}\n"
                 << "  config: {auth_config: {password_complexity: {min_length: " << 20 + i << "}}}\n";
        }
        auto doc = NFyaml::TDocument::Parse(yaml);
        UNIT_ASSERT(ValidationError(doc, &counter, true).empty());
        UNIT_ASSERT_VALUES_EQUAL(counter.Calls, (variants + 1) * (variants + 1));
        counter.Calls = 0;
        UNIT_ASSERT(ValidationError(doc, &counter, false).empty());
        UNIT_ASSERT_VALUES_EQUAL(counter.Calls, 2 * (variants + 1));
    }
}
