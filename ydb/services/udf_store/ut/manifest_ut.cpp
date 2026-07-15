#include <ydb/services/udf_store/wasm/manifest.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore::NWasm;

Y_UNIT_TEST_SUITE(TWasmManifestTest) {

Y_UNIT_TEST(ParseValidManifest) {
    const TString manifest = R"({
        "module_name": "LocalUdf",
        "calling_convention": "unversioned_value",
        "functions": [
            {
                "name": "udf_add",
                "argument_types": [
                    {"value": "int64", "tag": "concrete_type"},
                    {"value": "int64", "tag": "concrete_type"}
                ],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ],
        "required_libraries": []
    })";

    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.ModuleName, "LocalUdf");
    UNIT_ASSERT_VALUES_EQUAL(parsed.CallingConvention, "unversioned_value");
    UNIT_ASSERT(parsed.RequiredLibraries.empty());
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].Name, "udf_add");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].Args.size(), 2u);
    UNIT_ASSERT(parsed.Functions[0].Result == EUdfValueType::Int64);
}

Y_UNIT_TEST(ParseRequiredLibraries) {
    const TString manifest = R"({
        "module_name": "LocalUdf",
        "functions": [
            {
                "name": "udf_add",
                "argument_types": [],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ],
        "required_libraries": ["helpers-lib", "helpers"]
    })";

    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.RequiredLibraries.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.RequiredLibraries[0], "helpers-lib");
    UNIT_ASSERT_VALUES_EQUAL(parsed.RequiredLibraries[1], "helpers");
}

Y_UNIT_TEST(RejectEmptyManifest) {
    UNIT_ASSERT_EXCEPTION(ParseManifest(""), yexception);
}

} // Y_UNIT_TEST_SUITE
