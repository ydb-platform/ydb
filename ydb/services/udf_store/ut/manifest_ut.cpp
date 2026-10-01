#include <ydb/services/udf_store/wasm/manifest.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

using namespace NKikimr::NUdfStore::NWasm;

namespace {
TString Manifest(TStringBuf type) {
    return TStringBuilder() << R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
        "functions":[{"name":"f","result_type":)" << NJson::WriteJson(NJson::TJsonValue(type)) << "}]}";
}
TWasmTypeNodePtr Type(TStringBuf type) {
    return ParseManifest(Manifest(type)).Functions.at(0).ResultType;
}
}

Y_UNIT_TEST_SUITE(TWasmManifestTest) {
    Y_UNIT_TEST(ExactYqlTypes) {
        UNIT_ASSERT(Type("Int64")->Kind == TWasmTypeNode::EKind::Leaf);
        UNIT_ASSERT(Type("Int64")->Leaf == EUdfValueType::Int64);
        for (const auto name : {"Int64?", "Optional<Int64>"}) {
            const auto type = Type(name);
            UNIT_ASSERT(type->Kind == TWasmTypeNode::EKind::Optional);
            UNIT_ASSERT(type->Item->Leaf == EUdfValueType::Int64);
        }
        const auto dict = Type("Dict<String, List<Optional<Decimal(22,9)>>>");
        UNIT_ASSERT(dict->Kind == TWasmTypeNode::EKind::Dict);
        UNIT_ASSERT(dict->Key->Leaf == EUdfValueType::String);
        const auto decimal = dict->Payload->Item->Item;
        UNIT_ASSERT_VALUES_EQUAL(decimal->Precision, 22);
        UNIT_ASSERT_VALUES_EQUAL(decimal->Scale, 9);
    }
    Y_UNIT_TEST(SupportedLeaves) {
        for (const auto type : {"Null", "Bool", "Int32", "Uint32", "Int64", "Uint64", "Float", "Double",
            "String", "Utf8", "Date", "Datetime", "Timestamp", "Decimal(35,0)"}) {
            UNIT_ASSERT(Type(type)->Kind == TWasmTypeNode::EKind::Leaf);
        }
    }
    Y_UNIT_TEST(CanonicalMembersAndVariants) {
        for (const auto type : {"Struct<z: String, a: Int64>", "Variant<z: String, a: Int64>"}) {
            const auto node = Type(type);
            UNIT_ASSERT_VALUES_EQUAL(node->Members[0].Name, "a");
            UNIT_ASSERT_VALUES_EQUAL(node->Members[1].Name, "z");
            UNIT_ASSERT(node->Members[0].Type->Leaf == EUdfValueType::Int64);
        }
        UNIT_ASSERT(Type("Variant<a: Int64>")->NamedVariant);
        UNIT_ASSERT(!Type("Variant<Int64, String>")->NamedVariant);
        UNIT_ASSERT(Type("Tuple<String, Int64>")->Members[0].Type->Leaf == EUdfValueType::String);
        UNIT_ASSERT(Type("Struct<>")->Members.empty());
        UNIT_ASSERT_VALUES_EQUAL(Type("Resource<'a b'>")->Tag, "a b");
        UNIT_ASSERT_VALUES_EQUAL(Type("Resource<'a\\'<>b'>")->Tag, "a'<>b");
        UNIT_ASSERT(Type("Struct<'a<>': Optional<List<Int64>>>")->Members[0].Type->Kind == TWasmTypeNode::EKind::Optional);
        const auto callable = Type("(String, Int64?)->Bool");
        UNIT_ASSERT(callable->Kind == TWasmTypeNode::EKind::Callable);
        UNIT_ASSERT_VALUES_EQUAL(callable->Members.size(), 2);
        UNIT_ASSERT(callable->CallableReturns->Leaf == EUdfValueType::Boolean);
    }
    Y_UNIT_TEST(RejectInvalidAndUnsupportedTypes) {
        for (const auto type : {"", " ", "int64", "boolean", "String extra", "List<>", "List<Int64",
            "Decimal(0,0)", "Decimal(36,0)", "Decimal(10,11)", "Struct<a: Int64,a: String>",
            "Variant<>", "Dict<Resource<'r'>,Int64>", "Stream<Int64>", "Flow<Int64>",
            "Tagged<Int64,'t'>", "Int8", "(x:Int64)->Int64", "([Int64?])->Int64"}) {
            UNIT_ASSERT_EXCEPTION_C(Type(type), yexception, type);
        }
    }
    Y_UNIT_TEST(RejectOldJsonAndWrongFieldKinds) {
        for (const auto value : {R"({"value":"int64","tag":"concrete_type"})", "null", "[]", "7"}) {
            const TString manifest = TStringBuilder() << R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
                "functions":[{"name":"f","result_type":)" << value << "}]}";
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "YQL type string");
        }
        UNIT_ASSERT_EXCEPTION(ParseManifest(""), yexception);
        UNIT_ASSERT_EXCEPTION(ParseManifest(R"({"module_name":"Test"})"), yexception);
    }
    Y_UNIT_TEST(RejectAbiSelectorAtEveryLevel) {
        const TString function = R"({"name":"f","result_type":"Int64"})";
        const TString prefix = R"({"module_type":"module","module_kind":"wasm","module_name":"Test",)";
        for (const auto value : {"bridge", "unknown", "unversioned_value"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(prefix + "\"calling_convention\":\"" + value
                + "\",\"functions\":[" + function + "]}"), yexception, "calling_convention");
        }
        for (const auto body : {
            R"("functions":[{"name":"f","calling_convention":"bridge","result_type":"Int64"}])",
            R"("objects":[{"name":"X","create_export":"c","calling_convention":"bridge","methods":[{"name":"f","export":"f","result_type":"Int64"}]}])",
            R"("objects":[{"name":"X","create_export":"c","methods":[{"name":"f","export":"f","calling_convention":"bridge","result_type":"Int64"}]}])"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(prefix + body + "}"), yexception, "calling_convention");
        }
    }
    Y_UNIT_TEST(DepthBoundIncludesPostfixAndCallable) {
        TString nested = "Int64";
        for (ui32 i = 0; i < 32; ++i) { nested = "List<" + nested + ">"; }
        UNIT_ASSERT(Type(nested));
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type("List<" + nested + ">"), yexception, "Type nesting exceeds");
        UNIT_ASSERT(Type("Int64" + TString(32, '?')));
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type("Int64" + TString(33, '?')), yexception, "Type nesting exceeds");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type("(" + nested + ")->Int64"), yexception, "Type nesting exceeds");
        TString nestedCallable = "Int64";
        for (ui32 i = 0; i < 16; ++i) { nestedCallable = "(" + nestedCallable + ")->Int64"; }
        UNIT_ASSERT(Type(nestedCallable));
        TString hostile;
        for (size_t i = 0; i < 10000; ++i) { hostile += "List<"; }
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type(hostile), yexception, "Type lexical nesting exceeds");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type(TString(10000, '(')), yexception, "Type lexical nesting exceeds");
        TString mixedHostile;
        for (size_t i = 0; i < 300; ++i) { mixedHostile += "List<("; }
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type(mixedHostile), yexception, "Type lexical nesting exceeds");
        TString returnHostile;
        for (size_t i = 0; i < 10000; ++i) { returnHostile += "()->"; }
        returnHostile += "Int64";
        UNIT_ASSERT_EXCEPTION_CONTAINS(Type(returnHostile), yexception, "Type lexical nesting exceeds");
        TString siblings = "Tuple<";
        for (size_t i = 0; i < 300; ++i) {
            if (i) { siblings += ","; }
            siblings += "()->Int64";
        }
        siblings += ">";
        UNIT_ASSERT(Type(siblings));
    }
    Y_UNIT_TEST(ErrorsNameFunctionFieldAndPosition) {
        const TString manifest = R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
            "functions":[{"name":"Lookup","argument_types":["String","List<>"],"result_type":"Int64"}]})";
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "functions[0] (Lookup).argument_types[1]");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(manifest), yexception, "1:");
    }
    Y_UNIT_TEST(ObjectsSupportContainersAndConfiguredCallables) {
        const auto manifest = ParseManifest(R"({"module_type":"module","module_kind":"wasm","module_name":"Objects",
          "objects":[{"name":"X","create_export":"create","destroy_export":"destroy","methods":[
            {"name":"Apply","export":"apply","argument_types":["List<Int64>"],"result_type":"Dict<String,Int64>"},
            {"name":"Plain","export":"plain","yql_binding":"plain","argument_types":["Uint64"],"result_type":"String"}
          ]}]})");
        UNIT_ASSERT_VALUES_EQUAL(manifest.Functions.size(), 3);
        UNIT_ASSERT(manifest.Functions[0].IsObjectConstructor);
        UNIT_ASSERT_VALUES_EQUAL(manifest.Functions[0].Name, "New");
        UNIT_ASSERT(manifest.Functions[0].ResultType->Leaf == EUdfValueType::Uint64);
        UNIT_ASSERT(manifest.Functions[1].Binding == EWasmUdfBinding::TypeConfigCallable);
        UNIT_ASSERT(manifest.Functions[1].ArgTypes[0]->Kind == TWasmTypeNode::EKind::List);
        UNIT_ASSERT(manifest.Functions[2].Binding == EWasmUdfBinding::Plain);
    }
    Y_UNIT_TEST(NamesAndBindings) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
            "functions":[{"name":"f","yql_binding":"type_config_callable","result_type":"Int64"}]})"), yexception, "only supported under");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
            "functions":[{"name":"f","result_type":"Int64"},{"name":"f","result_type":"String"}]})"), yexception, "Duplicate YQL function name");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseManifest(R"({"module_type":"module","module_kind":"wasm","module_name":"Test",
            "functions":[{"name":"Apply","result_type":"Int64"}],
            "objects":[{"name":"X","create_export":"create","methods":[{"name":"Apply","export":"apply","result_type":"Int64"}]}]})"), yexception, "Duplicate YQL function name");
    }
}
