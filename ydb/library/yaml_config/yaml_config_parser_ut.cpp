#include "yaml_config_parser.h"
#include "yaml_config_parser_impl.h"
#include "yaml_config_helpers.h"

#include <ydb/core/protos/key.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NYaml;

Y_UNIT_TEST_SUITE(YamlConfigParser) {
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
        TString config = "{blob_storage_config: {autoconfig_settings: {erasure_species: block-4-2, pdisk_type: NVME}}, "
            "host_configs: [{nvme: [disk1, disk2]}], hosts: [{host: fqdn1}, {host: fqdn2}]}";
        NKikimrConfig::TAppConfig cfg = Parse(config, true);
        UNIT_ASSERT(cfg.HasBlobStorageConfig());
        auto& bsConfig = cfg.GetBlobStorageConfig();
        UNIT_ASSERT(bsConfig.HasAutoconfigSettings());
        auto& autoconf = bsConfig.GetAutoconfigSettings();
        UNIT_ASSERT(autoconf.HasDefineBox());
        UNIT_ASSERT_VALUES_EQUAL(autoconf.DefineHostConfigSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(autoconf.GetDefineBox().HostSize(), 2);
        for (const auto& host : autoconf.GetDefineBox().GetHost()) {
            UNIT_ASSERT_VALUES_EQUAL(host.GetHostConfigId(), autoconf.GetDefineHostConfig(0).GetHostConfigId());
        }
    }

    Y_UNIT_TEST(IncorrectHostConfigIdFails) {
        TString config1 = "{blob_storage_config: {autoconfig_settings: {erasure_species: block-4-2, pdisk_type: NVME}}, "
            "host_configs: [{nvme: [disk1, disk2]}], hosts: [{host: fqdn1, host_config_id: 2}, {host: fqdn2}]}";
        TString config2 = "{blob_storage_config: {autoconfig_settings: {erasure_species: block-4-2, pdisk_type: NVME}}, "
            "host_configs: [{host_config_id: 1, nvme: [disk1, disk2]}], hosts: [{host: fqdn1, host_config_id: 2}, "
            "{host: fqdn2}]}";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config1, false), yexception);
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config2, false), yexception);
    }

    Y_UNIT_TEST(NoMixedHostConfigIds) {
        TString config = "{blob_storage_config: {autoconfig_settings: {erasure_species: block-4-2, pdisk_type: NVME}}, "
            "host_configs: [{nvme: [disk1, disk2]}, {host_config_id: 2}], hosts: [{host: fqdn1, host_config_id: 2}, "
            "{host: fqdn2, host_config_id: 2}]}";
        UNIT_CHECK_GENERATED_EXCEPTION(Parse(config, false), yexception);
    }
}
