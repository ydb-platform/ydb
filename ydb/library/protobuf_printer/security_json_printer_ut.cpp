#include "security_json_printer.h"

#include <ydb/library/protobuf_printer/ut/test_proto.pb.h>
#include <ydb/library/protobuf_printer/ut/test_proto_required.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/config.h>
#include <library/cpp/testing/unittest/gtest.h>

using namespace NKikimr;

namespace {

NJson::TJsonValue ReadJsonFromString(TStringBuf json) {
    NJson::TJsonValue value;
    UNIT_ASSERT_C(NJson::ReadJsonTree(json, &value), TString(json));
    return value;
}

} // namespace

Y_UNIT_TEST_SUITE(SecurityJsonPrinterTest) {
    Y_UNIT_TEST(MasksSensitiveFields) {
        NTestProto::TConnectionContent m;
        m.set_name("name1");
        m.mutable_setting()->mutable_connection2()->set_database_id("db");
        m.mutable_setting()->mutable_connection2()->set_login("login1");
        m.mutable_setting()->mutable_connection2()->set_password("topsecret");

        NProtobufJson::TProto2JsonConfig cfg;
        cfg.UseJsonName = true;
        const TString actualJson = SecureProto2JsonString(m, cfg);
        EXPECT_EQ(
            ReadJsonFromString(R"json({
                "name": "name1",
                "setting": {
                    "connection2": {
                        "databaseId": "db",
                        "login":"***",
                        "password":"***"
                    }
                }})json"),
            ReadJsonFromString(actualJson));

        NTestProto::TComplexObject sens;
        sens.set_sens_bool(true);
        sens.set_sens_int(-17);
        sens.set_sens_double(3.5);
        sens.set_sens_string("secret");
        sens.add_arr2("a");
        sens.add_arr2("b");
        (*sens.mutable_map3())["a"] = "b";
        (*sens.mutable_map3())["c"] = "d";
        sens.clear_map2();

        NProtobufJson::TProto2JsonConfig cfgNullSingle;
        cfgNullSingle.UseJsonName = true;
        cfgNullSingle.MissingSingleKeyMode = NProtobufJson::TProto2JsonConfig::MissingKeyNull;
        const TString sampleJson = SecureProto2JsonString(sens, cfgNullSingle);
        EXPECT_EQ(
            ReadJsonFromString(R"json({
                "sensBool": "***",
                "sensInt": "***",
                "sensDouble": "***",
                "sensString": "***",
                "arr2": ["***", "***"],
                "map2": null,
                "map3": ["***", "***"]
            })json"),
            ReadJsonFromString(sampleJson));
    }

    Y_UNIT_TEST(PrintRecursiveType) {
        NTestProto::TRecursiveType response;
        response.set_name("name1");
        response.set_login("login1");
        response.add_types()->set_login("login2");
        response.add_types()->set_name("name3");

        NProtobufJson::TProto2JsonConfig cfg;
        cfg.UseJsonName = true;
        const TString actualJson = SecureProto2JsonString(response, cfg);
        EXPECT_EQ(
            ReadJsonFromString(R"json({
                "name": "name1",
                "login": "***",
                "types": [
                    {"login": "***" },
                    {"name": "name3"}
                ]})json"),
            ReadJsonFromString(actualJson));
    }

    Y_UNIT_TEST(NoThrowWithMissingKeyExplicitDefaultThrowRequired) {
        NTestProto::TObjectWithRequiredFields obj;
        obj.set_required_sensitive_no_default("secret");
        obj.set_required_non_sensitive_no_default("public");

        NProtobufJson::TProto2JsonConfig cfg;
        cfg.UseJsonName = true;
        cfg.MissingSingleKeyMode = NProtobufJson::TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;

        TString actualJson;
        EXPECT_NO_THROW(actualJson = SecureProto2JsonString(obj, cfg));
        EXPECT_EQ(
            ReadJsonFromString(R"json({
                "requiredSensitiveNoDefault": "***",
                "requiredNonSensitiveNoDefault": "public",
                "requiredNonSensitiveWithDefault": "non_sensitive_default"
            })json"),
            ReadJsonFromString(actualJson));
    }
}
