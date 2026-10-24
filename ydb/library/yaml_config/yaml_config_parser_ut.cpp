#include "yaml_config_parser.h"
#include "yaml_config_parser_impl.h"
#include "yaml_config_helpers.h"

#include <ydb/library/yaml_json/yaml_to_json.h>

#include <ydb/core/base/distconf_state.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NKikimr::NYaml;

Y_UNIT_TEST_SUITE(DistconfStateYaml) {
    using TNode = NKikimrConfig::TStaticNameserviceConfig::TNode;
    using NKikimr::NStorage::EDistconfState;
    using NKikimr::NStorage::ResolveDistconfNodeState;

    TString MakeConfig(TStringBuf hostState = {}, TStringBuf settings = {}) {
        return TStringBuilder() << R"(
erasure: mirror-3-dc
host_configs:
- host_config_id: 1
  drive: [{path: /dev/disk1, type: SSD}]
hosts:
- host: node-1
  node_id: 1
  host_config_id: 1
  location: {data_center: dc-1}
)" << hostState << R"(
- host: node-2
  node_id: 2
  host_config_id: 1
  location: {data_center: dc-2}
)" << settings;
    }

    Y_UNIT_TEST(AbsentMarkersPreserveDefaults) {
        const auto config = Parse(MakeConfig());
        UNIT_ASSERT(!config.HasSelfManagementConfig());
        UNIT_ASSERT(!config.GetSelfManagementConfig().GetOverrideDistconfState());
        for (const auto& node : config.GetNameserviceConfig().GetNode()) {
            UNIT_ASSERT(!node.HasDistconfState());
        }
    }

    Y_UNIT_TEST(HostStatePreservesAllHostsAndDisks) {
        const auto config = Parse(MakeConfig("  distconf_state: excluded\n"));
        const auto ordinary = Parse(MakeConfig());
        const auto& nodes = config.GetNameserviceConfig().GetNode();
        UNIT_ASSERT_VALUES_EQUAL(nodes.size(), 2);
        UNIT_ASSERT(nodes[0].GetDistconfState() == NKikimrConfig::TStaticNameserviceConfig::TNode::EXCLUDED);
        UNIT_ASSERT(!nodes[1].HasDistconfState());
        UNIT_ASSERT_VALUES_EQUAL(config.GetBlobStorageConfig().SerializeAsString(),
            ordinary.GetBlobStorageConfig().SerializeAsString());
    }

    Y_UNIT_TEST(DataCenterRulesArePreservedWithoutExpandingHosts) {
        const auto config = Parse(MakeConfig("  distconf_state: excluded\n",
            "self_management_config: {enabled: true, data_center_states: [{name: dc-2, state: excluded}]}\n"));
        const auto& states = config.GetSelfManagementConfig().GetDataCenterStates();
        UNIT_ASSERT_VALUES_EQUAL(states.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(states[0].GetName(), "dc-2");
        UNIT_ASSERT(states[0].HasState());
        UNIT_ASSERT(states[0].GetState() == NKikimrConfig::TStaticNameserviceConfig::TNode::EXCLUDED);
        UNIT_ASSERT_VALUES_EQUAL(config.GetNameserviceConfig().NodeSize(), 2);
        UNIT_ASSERT(!config.GetNameserviceConfig().GetNode(1).HasDistconfState());
    }

    Y_UNIT_TEST(ExplicitActiveStateIsPreserved) {
        const auto config = Parse(MakeConfig("  distconf_state: active\n"));
        const auto& node = config.GetNameserviceConfig().GetNode(0);
        UNIT_ASSERT(node.HasDistconfState());
        UNIT_ASSERT(node.GetDistconfState() == NKikimrConfig::TStaticNameserviceConfig::TNode::ACTIVE);
    }

    Y_UNIT_TEST(AbsentDataCenterStateIsDistinctFromExplicitActive) {
        const auto config = Parse(MakeConfig({}, R"(
self_management_config:
  enabled: true
  data_center_states:
  - {name: dc-1, state: active}
  - {name: dc-2}
)"));
        const auto& settings = config.GetSelfManagementConfig();
        UNIT_ASSERT(!settings.HasOverrideDistconfState());
        UNIT_ASSERT(!settings.GetOverrideDistconfState());
        const auto& states = settings.GetDataCenterStates();
        UNIT_ASSERT_VALUES_EQUAL(states.size(), 2);
        UNIT_ASSERT(states[0].HasState());
        UNIT_ASSERT(!states[1].HasState());
        UNIT_ASSERT(states[0].GetState() == NKikimrConfig::TStaticNameserviceConfig::TNode::ACTIVE);
        UNIT_ASSERT(states[1].GetState() == NKikimrConfig::TStaticNameserviceConfig::TNode::ACTIVE);
    }

    Y_UNIT_TEST(OverrideFlagPreservesExplicitStates) {
        for (bool overrideState : {false, true}) {
            const auto config = Parse(MakeConfig("  distconf_state: excluded\n", TStringBuilder()
                << "self_management_config:\n"
                << "  enabled: true\n"
                << "  override_distconf_state: " << (overrideState ? "true" : "false") << '\n'
                << "  data_center_states: [{name: dc-1, state: active}]\n"));
            const auto& settings = config.GetSelfManagementConfig();
            UNIT_ASSERT(settings.HasOverrideDistconfState());
            UNIT_ASSERT_VALUES_EQUAL(settings.GetOverrideDistconfState(), overrideState);
            UNIT_ASSERT(settings.GetDataCenterStates(0).GetState() == NKikimrConfig::TStaticNameserviceConfig::TNode::ACTIVE);
            const auto& nodes = config.GetNameserviceConfig().GetNode();
            UNIT_ASSERT(nodes[0].HasDistconfState());
            UNIT_ASSERT(nodes[0].GetDistconfState() == NKikimrConfig::TStaticNameserviceConfig::TNode::EXCLUDED);
            UNIT_ASSERT(!nodes[1].HasDistconfState());
        }
    }

    Y_UNIT_TEST(RejectsActiveHostInExcludedDataCenter) {
        for (const TStringBuf overrideState : {"false", "true"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Parse(MakeConfig("  distconf_state: active\n", TStringBuilder()
                << "self_management_config: {enabled: true, override_distconf_state: " << overrideState
                << ", data_center_states: [{name: dc-1, state: excluded}]}\n")), yexception,
                "active distconf host is in an excluded data center");
        }
    }

    Y_UNIT_TEST(RejectsUnknownStateAndUnstableNodeId) {
        UNIT_ASSERT_EXCEPTION(Parse(MakeConfig("  distconf_state: typo\n")), yexception);
        UNIT_ASSERT_EXCEPTION_CONTAINS(Parse("hosts: [{host: node-1, distconf_state: excluded}]"), yexception,
            "explicit nonzero node_id");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Parse("hosts: [{host: node-1, node_id: 0, distconf_state: excluded}]"), yexception,
            "explicit nonzero node_id");
    }

    Y_UNIT_TEST(RejectsInvalidDataCenterSelectors) {
        for (const TStringBuf selector : {"[{name: unknown, state: excluded}]", "[{name: '', state: active}]",
                "[{name: dc-1, state: active}, {name: dc-1, state: excluded}]"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Parse(MakeConfig({}, TStringBuilder()
                << "self_management_config: {enabled: true, data_center_states: " << selector << "}\n")), yexception,
                "distconf data center");
        }
        UNIT_ASSERT_EXCEPTION(Parse(MakeConfig({},
            "self_management_config: {enabled: true, data_center_states: [{name: dc-1, state: typo}]}\n")), yexception);
    }

    Y_UNIT_TEST(HostStateIsKeptWithExplicitNameservice) {
        const TString nameservice = R"(
nameservice_config:
  node:
  - {node_id: 1, host: node-1, interconnect_host: node-1, port: 19001, location: {data_center: dc-1}}
  - {node_id: 2, host: node-2, interconnect_host: node-2, port: 19001, location: {data_center: dc-2}}
)";
        const auto config = Parse(MakeConfig("  distconf_state: excluded\n", nameservice));
        UNIT_ASSERT(config.GetNameserviceConfig().GetNode(0).GetDistconfState() ==
            NKikimrConfig::TStaticNameserviceConfig::TNode::EXCLUDED);
        UNIT_ASSERT_EXCEPTION_CONTAINS(Parse(MakeConfig("  distconf_state: excluded\n", R"(
nameservice_config:
  node: [{node_id: 1, host: another-node, port: 19001}]
)")), yexception, "does not match nameservice");
    }

    Y_UNIT_TEST(EffectiveDefaultStateIsActive) {
        const auto config = Parse(MakeConfig());
        for (const auto& node : config.GetNameserviceConfig().GetNode()) {
            UNIT_ASSERT(ResolveDistconfNodeState(node, config.GetSelfManagementConfig()) == TNode::ACTIVE);
        }
    }

    Y_UNIT_TEST(DataCenterStateAffectsOnlyItsHosts) {
        const auto config = Parse(MakeConfig({},
            "self_management_config: {enabled: true, data_center_states: [{name: dc-1, state: excluded}]}\n"));
        const auto& nodes = config.GetNameserviceConfig().GetNode();
        UNIT_ASSERT(ResolveDistconfNodeState(nodes[0], config.GetSelfManagementConfig()) == TNode::EXCLUDED);
        UNIT_ASSERT(ResolveDistconfNodeState(nodes[1], config.GetSelfManagementConfig()) == TNode::ACTIVE);
    }

    Y_UNIT_TEST(StateSourcePriorityForHostsAndDataCenters) {
        const struct {
            TStringBuf Configured;
            std::optional<EDistconfState> Persisted;
            EDistconfState WithoutOverride;
            EDistconfState WithOverride;
        } cases[] = {
            {{}, std::nullopt, TNode::ACTIVE, TNode::ACTIVE},
            {{}, TNode::ACTIVE, TNode::ACTIVE, TNode::ACTIVE},
            {{}, TNode::EXCLUDED, TNode::EXCLUDED, TNode::EXCLUDED},
            {"active", std::nullopt, TNode::ACTIVE, TNode::ACTIVE},
            {"active", TNode::ACTIVE, TNode::ACTIVE, TNode::ACTIVE},
            {"active", TNode::EXCLUDED, TNode::EXCLUDED, TNode::ACTIVE},
            {"excluded", std::nullopt, TNode::EXCLUDED, TNode::EXCLUDED},
            {"excluded", TNode::ACTIVE, TNode::ACTIVE, TNode::EXCLUDED},
            {"excluded", TNode::EXCLUDED, TNode::EXCLUDED, TNode::EXCLUDED},
        };
        for (const auto& item : cases) {
            for (bool overrideState : {false, true}) {
                const auto expected = overrideState ? item.WithOverride : item.WithoutOverride;
                const TString settings = TStringBuilder() << "self_management_config:\n"
                    << "  enabled: true\n"
                    << "  override_distconf_state: " << (overrideState ? "true" : "false") << '\n';
                const TString hostState = item.Configured.empty() ? TString()
                    : TString(TStringBuilder() << "  distconf_state: " << item.Configured << '\n');
                const auto hostConfig = Parse(MakeConfig(hostState, settings));
                UNIT_ASSERT(ResolveDistconfNodeState(hostConfig.GetNameserviceConfig().GetNode(0),
                    hostConfig.GetSelfManagementConfig(), item.Persisted) == expected);

                const TString dataCenterState = TStringBuilder() << settings
                    << "  data_center_states:\n  - name: dc-1\n"
                    << (item.Configured.empty() ? TString()
                        : TString(TStringBuilder() << "    state: " << item.Configured << '\n'));
                const auto dcConfig = Parse(MakeConfig({}, dataCenterState));
                UNIT_ASSERT(ResolveDistconfNodeState(dcConfig.GetNameserviceConfig().GetNode(0),
                    dcConfig.GetSelfManagementConfig(), std::nullopt, item.Persisted) == expected);
            }
        }
    }

    Y_UNIT_TEST(ExplicitActiveDoesNotClearAnotherEntityExclusion) {
        const auto hostConfig = Parse(MakeConfig("  distconf_state: active\n",
            "self_management_config: {enabled: true, override_distconf_state: true}\n"));
        UNIT_ASSERT(ResolveDistconfNodeState(hostConfig.GetNameserviceConfig().GetNode(0),
            hostConfig.GetSelfManagementConfig(), TNode::EXCLUDED, TNode::EXCLUDED) == TNode::EXCLUDED);

        const auto dcConfig = Parse(MakeConfig({}, R"(
self_management_config:
  enabled: true
  override_distconf_state: true
  data_center_states: [{name: dc-1, state: active}]
)"));
        UNIT_ASSERT(ResolveDistconfNodeState(dcConfig.GetNameserviceConfig().GetNode(0),
            dcConfig.GetSelfManagementConfig(), TNode::EXCLUDED, TNode::EXCLUDED) == TNode::EXCLUDED);
    }

    Y_UNIT_TEST(ExplicitActiveCanRestoreBothEntities) {
        const auto config = Parse(MakeConfig("  distconf_state: active\n", R"(
self_management_config:
  enabled: true
  override_distconf_state: true
  data_center_states: [{name: dc-1, state: active}]
)"));
        UNIT_ASSERT(ResolveDistconfNodeState(config.GetNameserviceConfig().GetNode(0),
            config.GetSelfManagementConfig(), TNode::EXCLUDED, TNode::EXCLUDED) == TNode::ACTIVE);
    }

    Y_UNIT_TEST(EffectiveStateSupportsLegacyLocation) {
        auto config = Parse(MakeConfig({},
            "self_management_config: {enabled: true, data_center_states: [{name: dc-1, state: excluded}]}\n"));
        auto *node = config.MutableNameserviceConfig()->MutableNode(0);
        node->MutableWalleLocation()->CopyFrom(node->GetLocation());
        node->ClearLocation();
        UNIT_ASSERT(ResolveDistconfNodeState(*node, config.GetSelfManagementConfig()) == TNode::EXCLUDED);
    }
}

Y_UNIT_TEST_SUITE(YamlConfigParser) {
    Y_UNIT_TEST(EmptyOverrideMessagesKeepPresence) {
        const auto json = Yaml2Json(YAML::Load(R"(
blob_storage_config:
  storage_pool_pile_overrides:
    - pile_name: [dc2, dc3]
      all_kinds: true
      erasure_species: block-4-2
      geometry: {}
      pdisk_filters: {}
    - pile_name: [dc4]
      all_kinds: true
      erasure_species: block-4-2
)"), true);
        NKikimrConfig::TAppConfig config;
        Parse(json, GetJsonToProtoConfig(false), config, false);
        const auto& rules = config.GetBlobStorageConfig().GetStoragePoolPileOverrides();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].PileNameSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].GetPileName(0), "dc2");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].GetPileName(1), "dc3");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].PileNameSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].GetPileName(0), "dc4");
        UNIT_ASSERT(rules[0].HasGeometry());
        UNIT_ASSERT(rules[0].HasPDiskFilters());
        UNIT_ASSERT_VALUES_EQUAL(rules[0].GetPDiskFilters().PDiskFilterSize(), 0);
        UNIT_ASSERT(!rules[1].HasGeometry());
        UNIT_ASSERT(!rules[1].HasPDiskFilters());
    }

    Y_UNIT_TEST(Iterate) {
        NJson::TJsonValue e1;
        e1.SetType(NJson::EJsonValueType::JSON_MAP);

        NJson::TJsonValue e2;
        e2.SetType(NJson::EJsonValueType::JSON_MAP);

        NJson::TJsonValue e;
        e.SetType(NJson::EJsonValueType::JSON_ARRAY);
        e.AppendValue(e1);
        e.AppendValue(e1);

        NJson::TJsonValue d1;
        d1.SetType(NJson::EJsonValueType::JSON_MAP);

        NJson::TJsonValue d;
        d.SetType(NJson::EJsonValueType::JSON_ARRAY);
        d.AppendValue(d1);

        NJson::TJsonValue c1;
        c1.SetType(NJson::EJsonValueType::JSON_MAP);

        NJson::TJsonValue c2;
        c2.SetType(NJson::EJsonValueType::JSON_MAP);

        NJson::TJsonValue c;
        c.SetType(NJson::EJsonValueType::JSON_ARRAY);
        c.AppendValue(c1);
        c.AppendValue(c2);

        NJson::TJsonValue b;
        b.SetType(NJson::EJsonValueType::JSON_MAP);
        b["c"] = c;

        NJson::TJsonValue a;
        a.SetType(NJson::EJsonValueType::JSON_MAP);
        a["b"] = b;

        NJson::TJsonValue json;
        json.SetType(NJson::EJsonValueType::JSON_MAP);
        json["a"] = a;


        TString path = "/a/b/c/*/d/*/e/*";
        Iterate(json, path, [](const std::vector<ui32>& ids, const NJson::TJsonValue& node) {
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), 3);
            Y_UNUSED(node);
        });
    }

    Y_UNIT_TEST(ProtoBytesFieldDoesNotDecodeBase64) {
    // "c2FtcGxlLXBpbgo=" -> base64 decode -> "sample-pin"
            TString config = R"(
pdisk_key_config:
  keys:
  - container_path: "/a/b/c"
    pin: "c2FtcGxlLXBpbgo="
    id: "sample-encryption-key"
    version: 1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, false);

        UNIT_ASSERT(cfg.has_pdiskkeyconfig());
        auto keys = cfg.pdiskkeyconfig().GetKeys();
        UNIT_ASSERT_VALUES_EQUAL(keys.end() - keys.begin(), 1);
        auto key = keys.at(0);
        UNIT_ASSERT_VALUES_EQUAL("c2FtcGxlLXBpbgo=", key.pin());
    }

    Y_UNIT_TEST(FeatureFlagsWithTriboolAsBoolKeepsLaterFlags) {
        // Regression: EnableMvcc (tag 47) is a Tribool enum. A YAML boolean given
        // to it used to abort the whole FeatureFlags json->proto merge (which walks
        // fields in tag order), silently dropping every feature flag with a higher
        // tag, e.g. EnableArrowFormatAtDatashard (tag 49).
        TString config = R"(
feature_flags:
  enable_mvcc: true
  enable_arrow_format_at_datashard: true
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, false);

        UNIT_ASSERT(cfg.HasFeatureFlags());
        UNIT_ASSERT_C(cfg.GetFeatureFlags().GetEnableArrowFormatAtDatashard(),
            "EnableArrowFormatAtDatashard (tag 49) was dropped because a Tribool "
            "flag given as a YAML bool aborted the FeatureFlags parse");
    }

    Y_UNIT_TEST(FeatureFlagsWithoutTriboolParseFully) {
        // Control: without the Tribool-as-bool trigger the same high-tag flag
        // parses fine, isolating enable_mvcc as the cause.
        TString config = R"(
feature_flags:
  enable_arrow_format_at_datashard: true
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, false);

        UNIT_ASSERT(cfg.HasFeatureFlags());
        UNIT_ASSERT(cfg.GetFeatureFlags().GetEnableArrowFormatAtDatashard());
    }

    Y_UNIT_TEST(TriboolFeatureFlagAcceptsYamlBool) {
        // A Tribool feature flag may be written as a YAML boolean; true maps to
        // VALUE_TRUE and false maps to VALUE_FALSE.
        TString configTrue = R"(
feature_flags:
  enable_mvcc: true
)";
        NKikimrConfig::TAppConfig cfgTrue = Parse(configTrue, false);
        UNIT_ASSERT(cfgTrue.HasFeatureFlags());
        UNIT_ASSERT_VALUES_EQUAL((int)cfgTrue.GetFeatureFlags().GetEnableMvcc(),
            (int)NKikimrConfig::TFeatureFlags::VALUE_TRUE);

        TString configFalse = R"(
feature_flags:
  enable_mvcc: false
)";
        NKikimrConfig::TAppConfig cfgFalse = Parse(configFalse, false);
        UNIT_ASSERT(cfgFalse.HasFeatureFlags());
        UNIT_ASSERT_VALUES_EQUAL((int)cfgFalse.GetFeatureFlags().GetEnableMvcc(),
            (int)NKikimrConfig::TFeatureFlags::VALUE_FALSE);
    }

    Y_UNIT_TEST(TriboolFeatureFlagAcceptsEnumName) {
        // The explicit enum spelling keeps working alongside the boolean form.
        TString config = R"(
feature_flags:
  enable_mvcc: VALUE_FALSE
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, false);
        UNIT_ASSERT(cfg.HasFeatureFlags());
        UNIT_ASSERT_VALUES_EQUAL((int)cfg.GetFeatureFlags().GetEnableMvcc(),
            (int)NKikimrConfig::TFeatureFlags::VALUE_FALSE);
    }

    Y_UNIT_TEST(PdiskCategoryFromString) {
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("0"), 0ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("ROT"), 0ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("1"), 1ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("SSD"), 1ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("2"), 2ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("3"), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("562949953421312"), 1ull << 49);
        UNIT_ASSERT_VALUES_EQUAL(PdiskCategoryFromString("NVME"), 144115188075855873ull);
        UNIT_CHECK_GENERATED_EXCEPTION(PdiskCategoryFromString("zzz"), yexception);
        UNIT_CHECK_GENERATED_EXCEPTION(PdiskCategoryFromString("-1"), yexception);
    }

    Y_UNIT_TEST(AllowDefaultHostConfigId) {
        TString config = "{erasure: block-4-2, self_management_config: {enabled: true, erasure_species: block-4-2, pdisk_type: NVME}, "
            "host_configs: [{nvme: [disk1, disk2]}], hosts: [{host: fqdn1}, {host: fqdn2}]}";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasBlobStorageConfig());
        auto& bsConfig = cfg.GetBlobStorageConfig();
        UNIT_ASSERT(bsConfig.HasDefineBox());
        UNIT_ASSERT_VALUES_EQUAL(bsConfig.DefineHostConfigSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(bsConfig.GetDefineBox().HostSize(), 2);
        for (const auto& host : bsConfig.GetDefineBox().GetHost()) {
            UNIT_ASSERT_VALUES_EQUAL(host.GetHostConfigId(), bsConfig.GetDefineHostConfig(0).GetHostConfigId());
        }
    }

    Y_UNIT_TEST(IncorrectHostConfigIdFails) {
        TString config1 = "{self_management_config: {enabled: true, erasure_species: block-4-2, pdisk_type: NVME}, "
            "host_configs: [{nvme: [disk1, disk2]}], hosts: [{host: fqdn1, host_config_id: 2}, {host: fqdn2}]}";
        TString config2 = "{self_management_config: {enabled: true, erasure_species: block-4-2, pdisk_type: NVME}, "
            "host_configs: [{host_config_id: 1, nvme: [disk1, disk2]}], hosts: [{host: fqdn1, host_config_id: 2}, "
            "{host: fqdn2}]}";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config1, false), yexception);
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config2, false), yexception);
    }

    Y_UNIT_TEST(NoMixedHostConfigIds) {
        TString config = "{self_management_config: {enabled: true, erasure_species: block-4-2, pdisk_type: NVME}, "
            "host_configs: [{nvme: [disk1, disk2]}, {host_config_id: 2}], hosts: [{host: fqdn1, host_config_id: 2}, "
            "{host: fqdn2, host_config_id: 2}]}";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config, false), yexception);
    }

    Y_UNIT_TEST(ExpectedSlotSizeIsMutuallyExclusiveWithLegacySlotSettings) {
        TString withExpectedSlotCount = R"(
host_configs:
- host_config_id: 1
  drive:
  - path: disk1
    type: SSD
    expected_slot_count: 4
    expected_slot_size: 100000
hosts:
- host: fqdn1
  host_config_id: 1
)";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(withExpectedSlotCount, true), yexception);

        TString withSlotSizeInUnits = R"(
host_configs:
- host_config_id: 1
  drive:
  - path: disk1
    type: SSD
    slot_size_in_units: 2
    expected_slot_size: 100000
hosts:
- host: fqdn1
  host_config_id: 1
)";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(withSlotSizeInUnits, true), yexception);
    }

    Y_UNIT_TEST(ExpectedSlotSizeRequiresMaxSlots) {
        TString withoutMaxSlots = R"(
host_configs:
- host_config_id: 1
  drive:
  - path: disk1
    type: SSD
    expected_slot_size: 100000
hosts:
- host: fqdn1
  host_config_id: 1
)";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(withoutMaxSlots, true), yexception);

        TString withMaxSlots = R"(
host_configs:
- host_config_id: 1
  drive:
  - path: disk1
    type: SSD
    expected_slot_size: 100000
    max_slots: 8
hosts:
- host: fqdn1
  host_config_id: 1
)";
        NKikimrConfig::TAppConfig cfg = Parse(withMaxSlots, true);
        const auto& drive = cfg.GetBlobStorageConfig().GetDefineHostConfig(0).GetDrive(0);
        UNIT_ASSERT_VALUES_EQUAL(drive.GetPDiskConfig().GetExpectedSlotSize(), 100000);
        UNIT_ASSERT_VALUES_EQUAL(drive.GetPDiskConfig().GetMaxSlots(), 8);
    }

    Y_UNIT_TEST(MaxSlotsRequiresExpectedSlotSize) {
        TString withoutExpectedSlotSize = R"(
host_configs:
- host_config_id: 1
  drive:
  - path: disk1
    type: SSD
    max_slots: 8
hosts:
- host: fqdn1
  host_config_id: 1
)";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(withoutExpectedSlotSize, true), yexception);
    }

    Y_UNIT_TEST(StoragePoolTypesWithDefaultDomainName) {
        TString config = R"(
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "Root");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
    }

    Y_UNIT_TEST(StoragePoolTypesWithExplicitDomainName) {
        TString config = R"(
domain_name: MyCluster
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "MyCluster");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
    }

    Y_UNIT_TEST(StoragePoolTypesWithDomainsConfigName) {
        TString config = R"(
domains_config:
  domain:
    - name: CustomDomain
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "CustomDomain");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
    }

    Y_UNIT_TEST(DomainNameOverridesDomainsConfigName) {
        TString config = R"(
domain_name: ExplicitName
domains_config:
  domain:
    - name: ConfigName
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "ExplicitName");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
    }

    Y_UNIT_TEST(StoragePoolTypesWithDomainsConfigStoragePoolTypesFails) {
        TString config = R"(
domains_config:
  domain:
    - name: CustomDomain
      storage_pool_types:
        - kind: hdd
          pool_config:
            box_id: 1
            erasure_species: mirror-3-dc
            kind: hdd
            vdisk_kind: Default
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config, true), yexception);
    }

    Y_UNIT_TEST(StoragePoolTypesPreservesSecurityConfig) {
        TString config = R"(
domains_config:
  domain:
    - name: TestDomain
  security_config:
    enforce_user_token_requirement: true
    default_users:
      - name: admin
        password: secret
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "TestDomain");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
        UNIT_ASSERT(cfg.GetDomainsConfig().HasSecurityConfig());
        UNIT_ASSERT(cfg.GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetSecurityConfig().DefaultUsersSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetSecurityConfig().GetDefaultUsers(0).GetName(), "admin");
    }

    Y_UNIT_TEST(StoragePoolTypesWithExplicitDomainNamePreservesSecurityConfig) {
        TString config = R"(
domain_name: ExplicitDomain
domains_config:
  domain:
    - name: ConfigDomain
  security_config:
    enforce_user_token_requirement: true
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "ExplicitDomain");
        UNIT_ASSERT(cfg.GetDomainsConfig().HasSecurityConfig());
        UNIT_ASSERT(cfg.GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement());
    }

    Y_UNIT_TEST(MultipleStoragePoolTypesWithCustomDomain) {
        TString config = R"(
domain_name: MyCluster
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
  - kind: hdd
    pool_config:
      box_id: 1
      erasure_species: block-4-2
      kind: hdd
      vdisk_kind: Default
  - kind: nvme
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: nvme
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().DomainSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetName(), "MyCluster");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetStoragePoolTypes(0).GetKind(), "ssd");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetStoragePoolTypes(1).GetKind(), "hdd");
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetDomainsConfig().GetDomain(0).GetStoragePoolTypes(2).GetKind(), "nvme");
    }

    Y_UNIT_TEST(StoragePoolTypesWithMixedDiskTypesInHostConfigs) {
        TString config = R"(
domain_name: MyCluster
erasure: mirror-3-dc
storage_pool_types:
  - kind: rot
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: rot
      pdisk_filter:
        - property:
          - type: ROT
      vdisk_kind: Default
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      pdisk_filter:
        - property:
          - type: SSD
      vdisk_kind: Default
host_configs:
  - host_config_id: 1
    drive:
      - path: /dev/disk/by-partlabel/ydb_disk_1
        type: ROT
  - host_config_id: 2
    drive:
      - path: /dev/disk/by-partlabel/ydb_disk_2
        type: SSD
hosts:
  - host: node1
    host_config_id: 1
  - host: node2
    host_config_id: 2
)";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasDomainsConfig());
        const auto& domain = cfg.GetDomainsConfig().GetDomain(0);
        UNIT_ASSERT_VALUES_EQUAL(domain.GetName(), "MyCluster");
        UNIT_ASSERT_VALUES_EQUAL(domain.StoragePoolTypesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(domain.GetStoragePoolTypes(0).GetKind(), "rot");
        UNIT_ASSERT_VALUES_EQUAL(domain.GetStoragePoolTypes(0).GetPoolConfig().GetKind(), "rot");
        UNIT_ASSERT_VALUES_EQUAL(domain.GetStoragePoolTypes(1).GetKind(), "ssd");
        UNIT_ASSERT_VALUES_EQUAL(domain.GetStoragePoolTypes(1).GetPoolConfig().GetKind(), "ssd");
    }
}
