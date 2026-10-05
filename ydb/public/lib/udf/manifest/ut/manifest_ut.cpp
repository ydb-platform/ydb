#include <ydb/public/lib/udf/manifest/manifest.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/yexception.h>

using namespace NYdb::NUdfManifest;
Y_UNIT_TEST_SUITE(UdfManifest) {
Y_UNIT_TEST(ClassificationAndDefaults) {
    for (const auto type : {"module", "library"}) {
        for (const auto kind : {"wasm", "native"}) {
            NJson::TJsonValue json;
            json["module_name"] = "Example";
            json["module_type"] = type;
            json["module_kind"] = kind;
            const auto result = Parse(NJson::WriteJson(json));
            UNIT_ASSERT_VALUES_EQUAL(result.Name, "Example");
            UNIT_ASSERT_VALUES_EQUAL(result.Extension, "wasm");
            UNIT_ASSERT((result.Type == EModuleType::Library) == (TStringBuf(type) == "library"));
            UNIT_ASSERT((result.Kind == EModuleKind::Native) == (TStringBuf(kind) == "native"));
        }
    }
}
Y_UNIT_TEST(RequiredFieldsAndApplicability) {
    for (const auto bad : {
             "{}", "[]", "not json",
             R"({"module_name":"m","module_type":"module"})",
             R"({"module_name":"m","module_type":"udf","module_kind":"wasm"})",
             R"({"module_name":"m","module_type":"module","module_kind":"WASM"})",
             R"({"module_name":" ","module_type":"module","module_kind":"wasm"})",
             R"({"module_name":42,"module_type":"module","module_kind":"wasm"})",
             R"({"module_name":"m","module_type":"library","module_kind":"wasm","functions":[]})",
             R"({"module_name":"m","module_type":"library","module_kind":"wasm","objects":[]})",
             R"({"module_name":"m","module_type":"library","module_kind":"wasm","required_libraries":[]})",
             R"({"module_name":"m","module_type":"module","module_kind":"native","module_extension":"wasm"})",
             R"({"module_name":"m","module_type":"library","module_kind":"wasm","module_extension":"so"})"}) {
        UNIT_ASSERT_EXCEPTION(Parse(bad), yexception);
    }
}
} // Y_UNIT_TEST_SUITE(UdfManifest)
