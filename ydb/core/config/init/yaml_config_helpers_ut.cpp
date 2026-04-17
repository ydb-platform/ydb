#include "yaml_config_helpers.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NConfig;
using NYdb::NConsoleClient::TInitializationException;

Y_UNIT_TEST_SUITE(YamlConfigHelpers) {

    void AssertErrorCode(const TInitializationException& e, TStringBuf expected) {
        UNIT_ASSERT_C(e.HasErrorCode(), "error code missing");
        UNIT_ASSERT_VALUES_EQUAL(*e.GetErrorCode(), expected);
    }

    TString minimalValidConfig = R"(
metadata:
  cluster: test
  version: 1
config:
  domains_config:
    domain:
    - name: Root
      domain_id: 1
)";

    Y_UNIT_TEST(LoadYamlAsJson_ValidAndInvalid) {
        {
            auto json = LoadYamlAsJsonOrThrow(R"(
config:
  log_config:
    default_level: 3
)", "test");
            UNIT_ASSERT(json.Has("config"));
            UNIT_ASSERT(json["config"].Has("log_config"));
        }
        {
            try {
                LoadYamlAsJsonOrThrow("bad: [yaml: {broken", "my_config.yaml");
                UNIT_FAIL("Expected exception");
            } catch (const TInitializationException& e) {
                AssertErrorCode(e, "YDB-CFG01");
                TString msg = e.what();
                UNIT_ASSERT_C(msg.Contains("my_config.yaml"), "source missing: " << msg);
                UNIT_ASSERT_C(msg.Contains("Failed to parse"), "prefix missing: " << msg);
            }
        }
        {
            try {
                LoadYamlAsJsonOrThrow("bad: [yaml: {broken", {});
                UNIT_FAIL("Expected exception");
            } catch (const TInitializationException& e) {
                AssertErrorCode(e, "YDB-CFG01");
                TString msg = e.what();
                UNIT_ASSERT_C(msg.Contains("YAML config"), "default source missing: " << msg);
            }
        }
        {
            try {
                LoadYamlAsJsonOrThrow("config:\n  key: 1\n  key: 2\n", "dup.yaml");
                UNIT_FAIL("Expected exception on duplicate key");
            } catch (const TInitializationException& e) {
                AssertErrorCode(e, "YDB-CFG05");
                TString msg = e.what();
                UNIT_ASSERT_C(msg.Contains("duplicate key"), "duplicate key message missing: " << msg);
                UNIT_ASSERT_C(msg.Contains("dup.yaml"), "source missing: " << msg);
            }
        }
    }

    Y_UNIT_TEST(ParseJsonConfig_UnknownField) {
        TString yaml = minimalValidConfig + R"(  fake_field: 123)";
        auto json = LoadYamlAsJsonOrThrow(yaml, "test.yaml");
        NKikimrConfig::TAppConfig config;
        try {
            ParseJsonConfigOrThrow(json, "test.yaml", config);
            UNIT_FAIL("Expected exception");
        } catch (const TInitializationException& e) {
            AssertErrorCode(e, "YDB-CFG02");
            TString msg = e.what();
            UNIT_ASSERT_C(msg.Contains("fake_field"), "field name missing: " << msg);
            UNIT_ASSERT_C(msg.Contains("test.yaml"), "source missing: " << msg);
        }
    }

    Y_UNIT_TEST(ParseJsonConfig_ValidConfig) {
        TString yaml = minimalValidConfig + R"(  log_config:
    default_level: 3
)";
        auto json = LoadYamlAsJsonOrThrow(yaml, "test.yaml");
        NKikimrConfig::TAppConfig config;
        UNIT_ASSERT_NO_EXCEPTION(ParseJsonConfigOrThrow(json, "test.yaml", config));
        UNIT_ASSERT(config.HasLogConfig());
        UNIT_ASSERT_VALUES_EQUAL(config.GetLogConfig().GetDefaultLevel(), 3);
    }
}
