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

Y_UNIT_TEST(ParseObjectsTypeConfigCallable) {
    const TString manifest = R"({
        "module_name": "Prefix",
        "calling_convention": "unversioned_value",
        "required_libraries": ["sdk"],
        "objects": [
            {
                "name": "Prefix",
                "create_export": "prefix_create",
                "destroy_export": "prefix_destroy",
                "methods": [
                    {
                        "name": "Apply",
                        "export": "prefix_apply",
                        "yql_binding": "type_config_callable",
                        "argument_types": [
                            {"value": "string", "tag": "concrete_type"}
                        ],
                        "result_type": {"value": "string", "tag": "concrete_type"}
                    }
                ]
            }
        ]
    })";

    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Objects.size(), 1u);
    // New (from create_export) + Apply
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].Name, "New");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].ExportName, "prefix_create");
    UNIT_ASSERT(parsed.Functions[0].Binding == EWasmUdfBinding::Plain);
    UNIT_ASSERT(parsed.Functions[0].Result == EUdfValueType::Uint64);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].Name, "Apply");
    UNIT_ASSERT(parsed.Functions[1].Binding == EWasmUdfBinding::TypeConfigCallable);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].CreateExport, "prefix_create");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].CallExport, "prefix_apply");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].DestroyExport, "prefix_destroy");
}

Y_UNIT_TEST(ParseObjectsPlainSnapshot) {
    const TString manifest = R"({
        "module_name": "Ctx",
        "objects": [
            {
                "name": "Ctx",
                "create_export": "ctx_create",
                "destroy_export": "ctx_destroy",
                "methods": [
                    {
                        "name": "Snapshot",
                        "export": "ctx_snapshot",
                        "yql_binding": "plain",
                        "argument_types": [
                            {"value": "uint64", "tag": "concrete_type"}
                        ],
                        "result_type": {"value": "string", "tag": "concrete_type"}
                    }
                ]
            }
        ]
    })";

    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].Name, "New");
    UNIT_ASSERT_VALUES_EQUAL(TString(PlainWasmExport(parsed.Functions[0])), "ctx_create");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].Name, "Snapshot");
    UNIT_ASSERT(parsed.Functions[1].Binding == EWasmUdfBinding::Plain);
    UNIT_ASSERT_VALUES_EQUAL(TString(PlainWasmExport(parsed.Functions[1])), "ctx_snapshot");
}

Y_UNIT_TEST(SynthesizeNewObjectNameForSecondObject) {
    const TString manifest = R"({
        "module_name": "Multi",
        "objects": [
            {
                "name": "Foo",
                "create_export": "foo_create",
                "methods": [
                    {
                        "name": "FooRun",
                        "export": "foo_run",
                        "yql_binding": "plain",
                        "argument_types": [],
                        "result_type": {"value": "uint64", "tag": "concrete_type"}
                    }
                ]
            },
            {
                "name": "Bar",
                "create_export": "bar_create",
                "methods": [
                    {
                        "name": "BarRun",
                        "export": "bar_run",
                        "yql_binding": "plain",
                        "argument_types": [],
                        "result_type": {"value": "uint64", "tag": "concrete_type"}
                    }
                ]
            }
        ]
    })";

    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].Name, "New");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[0].ExportName, "foo_create");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[1].Name, "FooRun");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[2].Name, "NewBar");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[2].ExportName, "bar_create");
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions[3].Name, "BarRun");
}

Y_UNIT_TEST(RejectTypeConfigOnPlainFunctions) {
    const TString manifest = R"({
        "module_name": "Bad",
        "functions": [
            {
                "name": "x",
                "yql_binding": "type_config_callable",
                "argument_types": [],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ]
    })";
    UNIT_ASSERT_EXCEPTION(ParseManifest(manifest), yexception);
}

Y_UNIT_TEST(RejectEmptyManifest) {
    UNIT_ASSERT_EXCEPTION(ParseManifest(""), yexception);
}

Y_UNIT_TEST(RejectWideLeafUnderUnversionedValue) {
    // int32 has no TUnversionedValue slot, so it can only travel over the
    // bridge. Rejecting it here beats failing on the first row.
    const TString manifest = R"({
        "module_name": "Bad",
        "calling_convention": "unversioned_value",
        "functions": [
            {
                "name": "x",
                "argument_types": [{"value": "int32", "tag": "concrete_type"}],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ]
    })";
    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "int32");
}

Y_UNIT_TEST(RejectContainerResultUnderUnversionedValue) {
    const TString manifest = R"({
        "module_name": "Bad",
        "functions": [
            {
                "name": "x",
                "argument_types": [],
                "result_type": {
                    "value": "dict",
                    "key": {"value": "string", "tag": "concrete_type"},
                    "payload": {"value": "int64", "tag": "concrete_type"}
                }
            }
        ]
    })";
    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "result_type");
}

Y_UNIT_TEST(RejectWideTypeInObjectMethod) {
    // Object methods are always unversioned_value, whatever the module says.
    const TString manifest = R"({
        "module_name": "Bad",
        "calling_convention": "bridge",
        "objects": [
            {
                "name": "Ctx",
                "create_export": "ctx_create",
                "methods": [
                    {
                        "name": "Apply",
                        "export": "ctx_apply",
                        "yql_binding": "plain",
                        "argument_types": [{"value": "float", "tag": "concrete_type"}],
                        "result_type": {"value": "string", "tag": "concrete_type"}
                    }
                ]
            }
        ]
    })";
    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "float");
}

Y_UNIT_TEST(AcceptWideTypesUnderBridge) {
    const TString manifest = R"({
        "module_name": "Good",
        "calling_convention": "bridge",
        "functions": [
            {
                "name": "lookup",
                "argument_types": [
                    {
                        "value": "dict",
                        "key": {"value": "string", "tag": "concrete_type"},
                        "payload": {"value": "int64", "tag": "concrete_type"}
                    },
                    {"value": "int32", "tag": "concrete_type"}
                ],
                "result_type": {"value": "utf8", "tag": "concrete_type"}
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Functions.size(), 1u);
    UNIT_ASSERT(parsed.Functions[0].CallingConvention == EWasmCallingConvention::Bridge);
}

Y_UNIT_TEST(AcceptWideTypesUnderPerFunctionBridgeOverride) {
    const TString manifest = R"({
        "module_name": "Mixed",
        "calling_convention": "unversioned_value",
        "functions": [
            {
                "name": "lookup",
                "calling_convention": "bridge",
                "argument_types": [{"value": "int32", "tag": "concrete_type"}],
                "result_type": {"value": "int64", "tag": "concrete_type"}
            }
        ]
    })";
    const auto parsed = ParseManifest(manifest);
    UNIT_ASSERT(parsed.Functions[0].CallingConvention == EWasmCallingConvention::Bridge);
}

} // Y_UNIT_TEST_SUITE
