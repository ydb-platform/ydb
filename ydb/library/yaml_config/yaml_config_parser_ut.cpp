#include "yaml_config_parser.h"
#include "yaml_config_helpers.h"

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
}
