#include "hide_field_printer.h"
#include "protobuf_printer.h"
#include "security_json_printer.h"
#include "security_printer.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/config.h>
#include "size_printer.h"
#include "stream_helper.h"
#include "token_field_printer.h"
#include <ydb/library/protobuf_printer/ut/test_proto.pb.h>

#include <library/cpp/testing/unittest/gtest.h>

#include <util/string/builder.h>

using namespace NKikimr;

namespace {

NJson::TJsonValue ReadJsonFromString(TStringBuf json) {
    NJson::TJsonValue value;
    UNIT_ASSERT_C(NJson::ReadJsonTree(json, &value), TString(json));
    return value;
}

} // namespace

Y_UNIT_TEST_SUITE(PrinterWrapperTest) {
    Y_UNIT_TEST(PrintsToStream) {
        TStringBuilder s;
        NTestProto::TTestProto m;
        m.set_s1("xxx");
        s << TProtobufPrinterOutputWrapper(m, google::protobuf::TextFormat::Printer());
        UNIT_ASSERT_STRINGS_EQUAL(s, "s1: \"xxx\"\n");
    }

    Y_UNIT_TEST(PrintsToString) {
        NTestProto::TTestProto m;
        m.set_s1("xxx");
        const TString s = TProtobufPrinterOutputWrapper(m, google::protobuf::TextFormat::Printer());
        UNIT_ASSERT_STRINGS_EQUAL(s, "s1: \"xxx\"\n");
    }
}

Y_UNIT_TEST_SUITE(TokenPrinterTest) {
    Y_UNIT_TEST(PrintToken) {
        NTestProto::TTestProto m;
        m.set_token("123456789012345678901234567890");

        TCustomizableTextFormatPrinter printer;
        printer.RegisterFieldValuePrinters<NTestProto::TTestProto, TTokenFieldValuePrinter>("token");
        const TString s = TProtobufPrinterOutputWrapper(m, printer);
        UNIT_ASSERT_STRINGS_EQUAL(s, "token: \"1234****7890 (F229119D)\"\n");
    }
}

Y_UNIT_TEST_SUITE(HideFieldPrinterTest) {
    Y_UNIT_TEST(PrintNoValue) {
        NTestProto::TTestProto m;
        m.set_s1("trololo");
        m.set_s2("trololo");
        m.mutable_msg()->set_i(42);

        TCustomizableTextFormatPrinter printer;
        printer.RegisterFieldValuePrinters<NTestProto::TTestProto, THideFieldValuePrinter>("s1", "s2", "msg");
        {
            const TString s = TProtobufPrinterOutputWrapper(m, printer);
            UNIT_ASSERT_STRINGS_EQUAL(s, "s1: \"***\"\ns2: \"***\"\nmsg {\n  ***\n}\n");
        }

        printer.SetSingleLineMode(true);
        {
            const TString s = TProtobufPrinterOutputWrapper(m, printer);
            UNIT_ASSERT_STRINGS_EQUAL(s, "s1: \"***\" s2: \"***\" msg { *** } ");
        }
    }
}

Y_UNIT_TEST_SUITE(SecurityPrinterTest) {
    Y_UNIT_TEST(PrintSensitive) {
        NTestProto::TConnectionContent m;
        m.set_name("name1");
        m.mutable_setting()->mutable_connection2()->set_login("login1");
        m.mutable_setting()->mutable_connection2()->set_password("pswd");

        TSecurityTextFormatPrinter<NTestProto::TConnectionContent> printer;
        printer.SetSingleLineMode(true);
        {
            const TString s = TProtobufPrinterOutputWrapper(m, printer);
            UNIT_ASSERT_STRINGS_EQUAL(s, "name: \"name1\" setting { connection2 { login: \"***\" password: \"***\" } } ");
        }
    }

    Y_UNIT_TEST(PrintRecursiveType) {
        NTestProto::TRecursiveType response;
        response.set_name("name1");
        response.set_login("login1");
        response.add_types()->set_login("login2");
        response.add_types()->set_name("name3");
        TSecurityTextFormatPrinter<NTestProto::TRecursiveType> printer;
        printer.SetSingleLineMode(true);
        {
            const TString s = TProtobufPrinterOutputWrapper(response, printer);
            UNIT_ASSERT_STRINGS_EQUAL(s, "name: \"name1\" login: \"***\" types { login: \"***\" } types { name: \"name3\" } ");
        }
    }

    Y_UNIT_TEST(SecureDebugStringViaMessageRefSingleLine) {
        NTestProto::TConnectionContent m;
        m.set_name("name1");
        m.mutable_setting()->mutable_connection2()->set_login("login1");
        m.mutable_setting()->mutable_connection2()->set_password("pswd");
        const google::protobuf::Message& ref = m;
        const TString s = SecureDebugString(ref);
        UNIT_ASSERT_STRINGS_EQUAL(s, "name: \"name1\" setting { connection2 { login: \"***\" password: \"***\" } } ");
    }

    Y_UNIT_TEST(SecureDebugStringMultilineViaMessageRef) {
        NTestProto::TConnectionContent m;
        m.set_name("name1");
        m.mutable_setting()->mutable_connection2()->set_login("login1");
        m.mutable_setting()->mutable_connection2()->set_password("pswd");
        const google::protobuf::Message& ref = m;
        const TString s = SecureDebugStringMultiline(ref);
        const TString expected =
            "name: \"name1\"\n"
            "setting {\n"
            "  connection2 {\n"
            "    login: \"***\"\n"
            "    password: \"***\"\n"
            "  }\n"
            "}\n";
        UNIT_ASSERT_STRINGS_EQUAL(s, expected);
    }
}

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
        NTestProto::TComplexObject sens;
        sens.set_sens_string("secret");

        NProtobufJson::TProto2JsonConfig cfg;
        cfg.UseJsonName = true;
        cfg.MissingSingleKeyMode = NProtobufJson::TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;

        TString actualJson;
        EXPECT_NO_THROW(actualJson = SecureProto2JsonString(sens, cfg));
        const auto json = ReadJsonFromString(actualJson);

        EXPECT_TRUE(json.Has("sensString"));
        EXPECT_EQ(TString("***"), json["sensString"].GetStringSafe());
        EXPECT_FALSE(json.Has("map2"));
    }
}

Y_UNIT_TEST_SUITE(FieldSizePrinterTest) {
    Y_UNIT_TEST(PrintSuccess) {
        NTestProto::TBigObject bo;
        (*bo.mutable_map())["a1"] = "a";
        (*bo.mutable_map())["a2"] = "a2";
        (*bo.mutable_map())["a3"] = "a3";
        bo.add_list("b");
        bo.add_list("bb");
        bo.add_list("bbb");
        (*bo.mutable_object()->mutable_inner_map())["t"] = "1";
        bo.mutable_object()->set_value(2);
        bo.mutable_object()->mutable_inner()->add_a("aba");
        bo.mutable_object()->mutable_inner()->add_a("caba");
        UNIT_ASSERT_STRINGS_EQUAL(TSizeFormatPrinter(bo).ToString(), "map: 23 bytes list: 9 bytes object { inner_map: 6 bytes value: 8 bytes inner { a: 9 bytes } } ");
    }

    Y_UNIT_TEST(PrintRecursiveType) {
        NTestProto::TRecursiveType response;
        response.set_name("name1");
        response.set_login("login1");
        response.add_types()->set_login("login2");
        response.add_types()->set_name("name3");
        UNIT_ASSERT_STRINGS_EQUAL(TSizeFormatPrinter(response).ToString(), "name: 6 bytes login: 7 bytes types: 15 bytes ");
    }
}
