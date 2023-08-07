#include "json.h"
#include "proto.h"
#include "proto2json.h"

#include <library/cpp/protobuf/json/ut/test.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/ylimits.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

using namespace NProtobufJson;
using namespace NProtobufJsonTest;

namespace google {
    namespace protobuf {
        namespace internal {
            void MapTestForceDeterministic() {
                google::protobuf::io::CodedOutputStream::SetDefaultSerializationDeterministic();
            }
        }
    }     // namespace protobuf
}

namespace {
    class TInit {
    public:
        TInit() {
            ::google::protobuf::internal::MapTestForceDeterministic();
        }
    } Init;

    template <typename T>
    TString ConvertToString(T value) {
        return ToString(value);
    }

    // default ToString<double>() implementation loses precision
    TString ConvertToString(double value) {
        return FloatToString(value);
    }

    TString JsonValueToString(const NJson::TJsonValue& json) {
        NJsonWriter::TBuf buf(NJsonWriter::HEM_UNSAFE);
        return buf.WriteJsonValue(&json).Str();
    }

    void TestComplexMapAsObject(std::function<void(TComplexMapType&)>&& init, const TString& json, const TJson2ProtoConfig& config = TJson2ProtoConfig().SetMapAsObject(true)) {
        TComplexMapType modelProto;

        init(modelProto);

        TString modelStr(json);

        TComplexMapType proto;
        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TComplexMapType>(modelStr, config));

        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }
}

Y_UNIT_TEST_SUITE(TJson2ProtoTest) {
    Y_UNIT_TEST(TestFlatOptional){
        {const NJson::TJsonValue& json = CreateFlatJson();
    TFlatOptional proto;
    Json2Proto(json, proto);
    TFlatOptional modelProto;
    FillFlatProto(&modelProto);
    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
}

    // Try to skip each field
#define DEFINE_FIELD(name, value)                                     \
    {                                                                 \
        THashSet<TString> skippedField;                               \
        skippedField.insert(#name);                                   \
        const NJson::TJsonValue& json = CreateFlatJson(skippedField); \
        TFlatOptional proto;                                          \
        Json2Proto(json, proto);                                      \
        TFlatOptional modelProto;                                     \
        FillFlatProto(&modelProto, skippedField);                     \
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);                  \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestFlatOptional

Y_UNIT_TEST(TestFlatRequired){
    {const NJson::TJsonValue& json = CreateFlatJson();
TFlatRequired proto;
Json2Proto(json, proto);
TFlatRequired modelProto;
FillFlatProto(&modelProto);
UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                     \
    {                                                                 \
        THashSet<TString> skippedField;                               \
        skippedField.insert(#name);                                   \
        const NJson::TJsonValue& json = CreateFlatJson(skippedField); \
        TFlatRequired proto;                                          \
        UNIT_ASSERT_EXCEPTION(Json2Proto(json, proto), yexception);   \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestFlatRequired

Y_UNIT_TEST(TestNameGenerator) {
    TJson2ProtoConfig cfg;
    cfg.SetNameGenerator([](const NProtoBuf::FieldDescriptor&) { return "42"; });

    TNameGeneratorType proto;
    Json2Proto(TStringBuf(R"({"42":42})"), proto, cfg);

    TNameGeneratorType expected;
    expected.SetField(42);

    UNIT_ASSERT_PROTOS_EQUAL(expected, proto);
}

Y_UNIT_TEST(TestFlatNoCheckRequired) {
    {
        const NJson::TJsonValue& json = CreateFlatJson();
        TFlatRequired proto;
        Json2Proto(json, proto);
        TFlatRequired modelProto;
        FillFlatProto(&modelProto);
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }

    TJson2ProtoConfig cfg;
    cfg.CheckRequiredFields = false;

    // Try to skip each field
#define DEFINE_FIELD(name, value)                                     \
    {                                                                 \
        THashSet<TString> skippedField;                               \
        skippedField.insert(#name);                                   \
        const NJson::TJsonValue& json = CreateFlatJson(skippedField); \
        TFlatRequired proto;                                          \
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto, cfg));       \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestFlatNoCheckRequired

Y_UNIT_TEST(TestFlatRepeated){
    {const NJson::TJsonValue& json = CreateRepeatedFlatJson();
TFlatRepeated proto;
Json2Proto(json, proto);
TFlatRepeated modelProto;
FillRepeatedProto(&modelProto);
UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
}

// Try to skip each field
#define DEFINE_REPEATED_FIELD(name, ...)                                      \
    {                                                                         \
        THashSet<TString> skippedField;                                       \
        skippedField.insert(#name);                                           \
        const NJson::TJsonValue& json = CreateRepeatedFlatJson(skippedField); \
        TFlatRepeated proto;                                                  \
        Json2Proto(json, proto);                                              \
        TFlatRepeated modelProto;                                             \
        FillRepeatedProto(&modelProto, skippedField);                         \
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);                          \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD
} // TestFlatRepeated

Y_UNIT_TEST(TestCompositeOptional){
    {const NJson::TJsonValue& json = CreateCompositeJson();
TCompositeOptional proto;
Json2Proto(json, proto);
TCompositeOptional modelProto;
FillCompositeProto(&modelProto);
UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                          \
    {                                                                      \
        THashSet<TString> skippedField;                                    \
        skippedField.insert(#name);                                        \
        const NJson::TJsonValue& json = CreateCompositeJson(skippedField); \
        TCompositeOptional proto;                                          \
        Json2Proto(json, proto);                                           \
        TCompositeOptional modelProto;                                     \
        FillCompositeProto(&modelProto, skippedField);                     \
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);                       \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestCompositeOptional

Y_UNIT_TEST(TestCompositeOptionalStringBuf){
    {NJson::TJsonValue json = CreateCompositeJson();
json["Part"]["Double"] = 42.5;
TCompositeOptional proto;
Json2Proto(JsonValueToString(json), proto);
TCompositeOptional modelProto;
FillCompositeProto(&modelProto);
modelProto.MutablePart()->SetDouble(42.5);
UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                   \
    {                                                               \
        THashSet<TString> skippedField;                             \
        skippedField.insert(#name);                                 \
        NJson::TJsonValue json = CreateCompositeJson(skippedField); \
        if (json["Part"].Has("Double")) {                           \
            json["Part"]["Double"] = 42.5;                          \
        }                                                           \
        TCompositeOptional proto;                                   \
        Json2Proto(JsonValueToString(json), proto);                 \
        TCompositeOptional modelProto;                              \
        FillCompositeProto(&modelProto, skippedField);              \
        if (modelProto.GetPart().HasDouble()) {                     \
            modelProto.MutablePart()->SetDouble(42.5);              \
        }                                                           \
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);                \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestCompositeOptionalStringBuf

Y_UNIT_TEST(TestCompositeRequired) {
    {
        const NJson::TJsonValue& json = CreateCompositeJson();
        TCompositeRequired proto;
        Json2Proto(json, proto);
        TCompositeRequired modelProto;
        FillCompositeProto(&modelProto);
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }

    {
        NJson::TJsonValue json;
        TCompositeRequired proto;
        UNIT_ASSERT_EXCEPTION(Json2Proto(json, proto), yexception);
    }
} // TestCompositeRequired

Y_UNIT_TEST(TestCompositeRepeated) {
    {
        NJson::TJsonValue json;
        NJson::TJsonValue array;
        array.AppendValue(CreateFlatJson());
        json.InsertValue("Part", array);

        TCompositeRepeated proto;
        Json2Proto(json, proto);

        TFlatOptional partModelProto;
        FillFlatProto(&partModelProto);
        TCompositeRepeated modelProto;
        modelProto.AddPart()->CopyFrom(partModelProto);

        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }

    {
        // Array of messages with each field skipped
        TCompositeRepeated modelProto;
        NJson::TJsonValue array;

#define DEFINE_REPEATED_FIELD(name, ...)                 \
    {                                                    \
        THashSet<TString> skippedField;                  \
        skippedField.insert(#name);                      \
        TFlatOptional partModelProto;                    \
        FillFlatProto(&partModelProto, skippedField);    \
        modelProto.AddPart()->CopyFrom(partModelProto);  \
        array.AppendValue(CreateFlatJson(skippedField)); \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD

        NJson::TJsonValue json;
        json.InsertValue("Part", array);

        TCompositeRepeated proto;
        Json2Proto(json, proto);

        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }
} // TestCompositeRepeated

Y_UNIT_TEST(TestInvalidEnum) {
    {
        NJson::TJsonValue json;
        json.InsertValue("Enum", "E_100");
        TFlatOptional proto;
        UNIT_ASSERT_EXCEPTION(Json2Proto(json, proto), yexception);
    }

    {
        NJson::TJsonValue json;
        json.InsertValue("Enum", 100);
        TFlatOptional proto;
        UNIT_ASSERT_EXCEPTION(Json2Proto(json, proto), yexception);
    }
}

Y_UNIT_TEST(TestFieldNameMode) {
    // Original case 1
    {
        TString modelStr(R"_({"String":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }

    // Original case 2
    {
        TString modelStr(R"_({"String":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameOriginalCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }

    // Lowercase
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }

    // Uppercase
    {
        TString modelStr(R"_({"STRING":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameUpperCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }

    // Camelcase
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }
    {
        TString modelStr(R"_({"oneString":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetOneString() == "value");
    }
    {
        TString modelStr(R"_({"oneTwoString":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetOneTwoString() == "value");
    }

    // snake_case
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value");
    }
    {
        TString modelStr(R"_({"one_string":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetOneString() == "value");
    }
    {
        TString modelStr(R"_({"one_two_string":"value"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetOneTwoString() == "value");
    }

    // Original case, repeated
    {
        TString modelStr(R"_({"I32":[1,2]})_");

        TFlatRepeated proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameOriginalCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatRepeated>(modelStr, config));
        UNIT_ASSERT(proto.I32Size() == 2);
        UNIT_ASSERT(proto.GetI32(0) == 1);
        UNIT_ASSERT(proto.GetI32(1) == 2);
    }

    // Lower case, repeated
    {
        TString modelStr(R"_({"i32":[1,2]})_");

        TFlatRepeated proto;
        TJson2ProtoConfig config;
        config.FieldNameMode = TJson2ProtoConfig::FieldNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatRepeated>(modelStr, config));
        UNIT_ASSERT(proto.I32Size() == 2);
        UNIT_ASSERT(proto.GetI32(0) == 1);
        UNIT_ASSERT(proto.GetI32(1) == 2);
    }

    // UseJsonName
    {
        // FIXME(CONTRIB-139): since protobuf 3.1, Def_upper json name is
        // "DefUpper", but until kernel/ugc/schema and yweb/yasap/pdb are
        // updated, library/cpp/protobuf/json preserves compatibility with
        // protobuf 3.0 by lowercasing default names, making it "defUpper".
        TString modelStr(R"_({"My-Upper":1,"my-lower":2,"defUpper":3,"defLower":4})_");

        TWithJsonName proto;
        TJson2ProtoConfig config;
        config.SetUseJsonName(true);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TWithJsonName>(modelStr, config));
        UNIT_ASSERT_EQUAL(proto.Getmy_upper(), 1);
        UNIT_ASSERT_EQUAL(proto.GetMy_lower(), 2);
        UNIT_ASSERT_EQUAL(proto.GetDef_upper(), 3);
        UNIT_ASSERT_EQUAL(proto.Getdef_lower(), 4);
    }

    // UseJsonName with UseJsonEnumValue
    {
        TString modelStr(R"_({"json_enum" : "enum_1"})_");

        TCustomJsonEnumValue proto;
        TJson2ProtoConfig config;
        config.SetUseJsonName(true);
        config.SetUseJsonEnumValue(true);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TCustomJsonEnumValue>(modelStr, config));
        UNIT_ASSERT_EQUAL(proto.GetJsonEnum(), EJsonEnum::J_1);
    }

    // FieldNameMode with UseJsonName
    {
        TJson2ProtoConfig config;
        config.SetFieldNameMode(TJson2ProtoConfig::FieldNameLowerCase);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.SetUseJsonName(true), yexception, "mutually exclusive");
    }
    {
        TJson2ProtoConfig config;
        config.SetUseJsonName(true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.SetFieldNameMode(TJson2ProtoConfig::FieldNameLowerCase), yexception, "mutually exclusive");
    }
} // TestFieldNameMode

class TStringTransform: public IStringTransform {
public:
    int GetType() const override {
        return 0;
    }
    void Transform(TString& str) const override {
        str = "transformed_any";
    }
};

class TBytesTransform: public IStringTransform {
public:
    int GetType() const override {
        return 0;
    }
    void Transform(TString&) const override {
    }
    void TransformBytes(TString& str) const override {
        str = "transformed_bytes";
    }
};

Y_UNIT_TEST(TestInvalidJson) {
    NJson::TJsonValue val{"bad value"};
    TFlatOptional proto;
    UNIT_ASSERT_EXCEPTION(Json2Proto(val, proto), yexception);
}

Y_UNIT_TEST(TestInvalidRepeatedFieldWithMapAsObject) {
    TCompositeRepeated proto;
    TJson2ProtoConfig config;
    config.MapAsObject = true;
    UNIT_ASSERT_EXCEPTION(Json2Proto(TStringBuf(R"({"Part":{"Boo":{}}})"), proto, config), yexception);
}

Y_UNIT_TEST(TestStringTransforms) {
    // Check that strings and bytes are transformed
    {
        TString modelStr(R"_({"String":"value_str", "Bytes": "value_bytes"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.AddStringTransform(new TStringTransform);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "transformed_any");
        UNIT_ASSERT(proto.GetBytes() == "transformed_any");
    }

    // Check that bytes are transformed, strings are left intact
    {
        TString modelStr(R"_({"String":"value_str", "Bytes": "value_bytes"})_");

        TFlatOptional proto;
        TJson2ProtoConfig config;
        config.AddStringTransform(new TBytesTransform);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetString() == "value_str");
        UNIT_ASSERT(proto.GetBytes() == "transformed_bytes");
    }

    // Check that repeated bytes are transformed, repeated strings are left intact
    {
        TString modelStr(R"_({"String":["value_str", "str2"], "Bytes": ["value_bytes", "bytes2"]})_");

        TFlatRepeated proto;
        TJson2ProtoConfig config;
        config.AddStringTransform(new TBytesTransform);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatRepeated>(modelStr, config));
        UNIT_ASSERT(proto.StringSize() == 2);
        UNIT_ASSERT(proto.GetString(0) == "value_str");
        UNIT_ASSERT(proto.GetString(1) == "str2");
        UNIT_ASSERT(proto.BytesSize() == 2);
        UNIT_ASSERT(proto.GetBytes(0) == "transformed_bytes");
        UNIT_ASSERT(proto.GetBytes(1) == "transformed_bytes");
    }

    // Check that bytes are transformed, strings are left intact in composed messages
    {
        TString modelStr(R"_({"Part": {"String":"value_str", "Bytes": "value_bytes"}})_");

        TCompositeOptional proto;
        TJson2ProtoConfig config;
        config.AddStringTransform(new TBytesTransform);

        UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TCompositeOptional>(modelStr, config));
        UNIT_ASSERT(proto.GetPart().GetString() == "value_str");
        UNIT_ASSERT(proto.GetPart().GetBytes() == "transformed_bytes");
    }
} // TestStringTransforms

Y_UNIT_TEST(TestCastFromString) {
    // single fields
    {
        NJson::TJsonValue json;
#define DEFINE_FIELD(name, value) \
    json.InsertValue(#name, ConvertToString(value));
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD

        TFlatOptional proto;
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto(json, proto), yexception, "Invalid type");

        TJson2ProtoConfig config;
        config.SetCastFromString(true);
        Json2Proto(json, proto, config);

        TFlatOptional modelProto;
        FillFlatProto(&modelProto);
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }

    // repeated fields
    {
        NJson::TJsonValue json;
#define DEFINE_REPEATED_FIELD(name, type, ...)                         \
    {                                                                  \
        type values[] = {__VA_ARGS__};                                 \
        NJson::TJsonValue array(NJson::JSON_ARRAY);                    \
        for (size_t i = 0, end = Y_ARRAY_SIZE(values); i < end; ++i) { \
            array.AppendValue(ConvertToString(values[i]));             \
        }                                                              \
        json.InsertValue(#name, array);                                \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD

        TFlatRepeated proto;
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto(json, proto), yexception, "Invalid type");

        TJson2ProtoConfig config;
        config.SetCastFromString(true);
        Json2Proto(json, proto, config);

        TFlatRepeated modelProto;
        FillRepeatedProto(&modelProto);
        UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
    }
} // TestCastFromString

Y_UNIT_TEST(TestMap) {
    TMapType modelProto;

    auto& items = *modelProto.MutableItems();
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    TString modelStr(R"_({"Items":[{"key":"key3","value":"value3"},{"key":"key2","value":"value2"},{"key":"key1","value":"value1"}]})_");

    TJson2ProtoConfig config;
    TMapType proto;
    UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TMapType>(modelStr, config));

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMap

Y_UNIT_TEST(TestCastRobust) {
    NJson::TJsonValue json;
    json["I32"] = "5";
    json["Bool"] = 1;
    json["String"] = 6;
    json["Double"] = 8;
    TFlatOptional proto;
    UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto(json, proto), yexception, "Invalid type");

    TJson2ProtoConfig config;
    config.SetCastRobust(true);
    Json2Proto(json, proto, config);

    TFlatOptional expected;
    expected.SetI32(5);
    expected.SetBool(true);
    expected.SetString("6");
    expected.SetDouble(8);
    UNIT_ASSERT_PROTOS_EQUAL(proto, expected);
}

Y_UNIT_TEST(TestVectorizeScalars) {
    NJson::TJsonValue json;
#define DEFINE_FIELD(name, value) \
    json.InsertValue(#name, value);
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD

    TFlatRepeated proto;
    TJson2ProtoConfig config;
    config.SetVectorizeScalars(true);
    Json2Proto(json, proto, config);

#define DEFINE_FIELD(name, value) \
    UNIT_ASSERT_VALUES_EQUAL(proto.Get ## name(0), value);
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
}

Y_UNIT_TEST(TestValueVectorizer) {
    {
        // No ValueVectorizer
        NJson::TJsonValue json;
        json["RepeatedString"] = "123";
        TJson2ProtoConfig config;
        TSingleRepeatedString expected;
        UNIT_ASSERT_EXCEPTION(Json2Proto(json, expected, config), yexception);
    }
    {
        // ValueVectorizer replace original value by array
        NJson::TJsonValue json;
        json["RepeatedString"] = "123";
        TJson2ProtoConfig config;

        TSingleRepeatedString expected;
        expected.AddRepeatedString("4");
        expected.AddRepeatedString("5");
        expected.AddRepeatedString("6");

        config.ValueVectorizer = [](const NJson::TJsonValue& val) -> NJson::TJsonValue::TArray {
            Y_UNUSED(val);
            return {NJson::TJsonValue("4"), NJson::TJsonValue("5"), NJson::TJsonValue("6")};
        };
        TSingleRepeatedString actual;
        Json2Proto(json, actual, config);
        UNIT_ASSERT_PROTOS_EQUAL(expected, actual);
    }
    {
        // ValueVectorizer replace original value by array and cast
        NJson::TJsonValue json;
        json["RepeatedInt"] = 123;
        TJson2ProtoConfig config;

        TSingleRepeatedInt expected;
        expected.AddRepeatedInt(4);
        expected.AddRepeatedInt(5);
        expected.AddRepeatedInt(6);

        config.ValueVectorizer = [](const NJson::TJsonValue& val) -> NJson::TJsonValue::TArray {
            Y_UNUSED(val);
            return {NJson::TJsonValue("4"), NJson::TJsonValue(5), NJson::TJsonValue("6")};
        };
        config.CastFromString = true;

        TSingleRepeatedInt actual;
        Json2Proto(json, actual, config);
        UNIT_ASSERT_PROTOS_EQUAL(expected, actual);
    }
}

Y_UNIT_TEST(TestMapAsObject) {
    TMapType modelProto;

    auto& items = *modelProto.MutableItems();
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    TString modelStr(R"_({"Items":{"key1":"value1","key2":"value2","key3":"value3"}})_");

    TJson2ProtoConfig config;
    config.MapAsObject = true;
    TMapType proto;
    UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TMapType>(modelStr, config));

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMapAsObject

Y_UNIT_TEST(TestComplexMapAsObject_I32) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableI32();
            items[1] = 1;
            items[-2] = -2;
            items[3] = 3;
        },
        R"_({"I32":{"1":1,"-2":-2,"3":3}})_");
} // TestComplexMapAsObject_I32

Y_UNIT_TEST(TestComplexMapAsObject_I64) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableI64();
            items[2147483649L] = 2147483649L;
            items[-2147483650L] = -2147483650L;
            items[2147483651L] = 2147483651L;
        },
        R"_({"I64":{"2147483649":2147483649,"-2147483650":-2147483650,"2147483651":2147483651}})_");
} // TestComplexMapAsObject_I64

Y_UNIT_TEST(TestComplexMapAsObject_UI32) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableUI32();
            items[1073741825U] = 1073741825U;
            items[1073741826U] = 1073741826U;
            items[1073741827U] = 1073741827U;
        },
        R"_({"UI32":{"1073741825":1073741825,"1073741826":1073741826,"1073741827":1073741827}})_");
} // TestComplexMapAsObject_UI32

Y_UNIT_TEST(TestComplexMapAsObject_UI64) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableUI64();
            items[9223372036854775809UL] = 9223372036854775809UL;
            items[9223372036854775810UL] = 9223372036854775810UL;
            items[9223372036854775811UL] = 9223372036854775811UL;
        },
        R"_({"UI64":{"9223372036854775809":9223372036854775809,"9223372036854775810":9223372036854775810,"9223372036854775811":9223372036854775811}})_");
} // TestComplexMapAsObject_UI64

Y_UNIT_TEST(TestComplexMapAsObject_SI32) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableSI32();
            items[1] = 1;
            items[-2] = -2;
            items[3] = 3;
        },
        R"_({"SI32":{"1":1,"-2":-2,"3":3}})_");
} // TestComplexMapAsObject_SI32

Y_UNIT_TEST(TestComplexMapAsObject_SI64) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableSI64();
            items[2147483649L] = 2147483649L;
            items[-2147483650L] = -2147483650L;
            items[2147483651L] = 2147483651L;
        },
        R"_({"SI64":{"2147483649":2147483649,"-2147483650":-2147483650,"2147483651":2147483651}})_");
} // TestComplexMapAsObject_SI64

Y_UNIT_TEST(TestComplexMapAsObject_FI32) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableFI32();
            items[1073741825U] = 1073741825U;
            items[1073741826U] = 1073741826U;
            items[1073741827U] = 1073741827U;
        },
        R"_({"FI32":{"1073741825":1073741825,"1073741826":1073741826,"1073741827":1073741827}})_");
} // TestComplexMapAsObject_FI32

Y_UNIT_TEST(TestComplexMapAsObject_FI64) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableFI64();
            items[9223372036854775809UL] = 9223372036854775809UL;
            items[9223372036854775810UL] = 9223372036854775810UL;
            items[9223372036854775811UL] = 9223372036854775811UL;
        },
        R"_({"FI64":{"9223372036854775809":9223372036854775809,"9223372036854775810":9223372036854775810,"9223372036854775811":9223372036854775811}})_");
} // TestComplexMapAsObject_FI64

Y_UNIT_TEST(TestComplexMapAsObject_SFI32) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableSFI32();
            items[1] = 1;
            items[-2] = -2;
            items[3] = 3;
        },
        R"_({"SFI32":{"1":1,"-2":-2,"3":3}})_");
} // TestComplexMapAsObject_SFI32

Y_UNIT_TEST(TestComplexMapAsObject_SFI64) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableSFI64();
            items[2147483649L] = 2147483649L;
            items[-2147483650L] = -2147483650L;
            items[2147483651L] = 2147483651L;
        },
        R"_({"SFI64":{"2147483649":2147483649,"-2147483650":-2147483650,"2147483651":2147483651}})_");
} // TestComplexMapAsObject_SFI64

Y_UNIT_TEST(TestComplexMapAsObject_Bool) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableBool();
            items[true] = true;
            items[false] = false;
        },
        R"_({"Bool":{"true":true,"false":false}})_");
} // TestComplexMapAsObject_Bool

Y_UNIT_TEST(TestComplexMapAsObject_String) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableString();
            items["key1"] = "value1";
            items["key2"] = "value2";
            items["key3"] = "value3";
            items[""] = "value4";
        },
        R"_({"String":{"key1":"value1","key2":"value2","key3":"value3","":"value4"}})_");
} // TestComplexMapAsObject_String

Y_UNIT_TEST(TestComplexMapAsObject_Enum) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableEnum();
            items["key1"] = EEnum::E_1;
            items["key2"] = EEnum::E_2;
            items["key3"] = EEnum::E_3;
        },
        R"_({"Enum":{"key1":1,"key2":2,"key3":3}})_");
} // TestComplexMapAsObject_Enum

Y_UNIT_TEST(TestComplexMapAsObject_EnumString) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableEnum();
            items["key1"] = EEnum::E_1;
            items["key2"] = EEnum::E_2;
            items["key3"] = EEnum::E_3;
        },
        R"_({"Enum":{"key1":"E_1","key2":"E_2","key3":"E_3"}})_");
} // TestComplexMapAsObject_EnumString

Y_UNIT_TEST(TestComplexMapAsObject_EnumStringCaseInsensetive) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableEnum();
            items["key1"] = EEnum::E_1;
            items["key2"] = EEnum::E_2;
            items["key3"] = EEnum::E_3;
        },
        R"_({"Enum":{"key1":"e_1","key2":"E_2","key3":"e_3"}})_",
        TJson2ProtoConfig()
                .SetMapAsObject(true)
                .SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive)
    );
} // TestComplexMapAsObject_EnumStringCaseInsensetive

Y_UNIT_TEST(TestComplexMapAsObject_EnumStringSnakeCaseInsensitive) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableEnum();
            items["key1"] = EEnum::E_1;
            items["key2"] = EEnum::E_2;
            items["key3"] = EEnum::E_3;
        },
        R"_({"Enum":{"key1":"e1","key2":"_E_2_","key3":"e_3"}})_",
        TJson2ProtoConfig()
                .SetMapAsObject(true)
                .SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive)
    );
} // TestComplexMapAsObject_EnumStringCaseInsensetive

Y_UNIT_TEST(TestComplexMapAsObject_Float) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableFloat();
            items["key1"] = 0.1f;
            items["key2"] = 0.2f;
            items["key3"] = 0.3f;
        },
        R"_({"Float":{"key1":0.1,"key2":0.2,"key3":0.3}})_");
} // TestComplexMapAsObject_Float

Y_UNIT_TEST(TestComplexMapAsObject_Double) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            auto& items = *proto.MutableDouble();
            items["key1"] = 0.1L;
            items["key2"] = 0.2L;
            items["key3"] = 0.3L;
        },
        R"_({"Double":{"key1":0.1,"key2":0.2,"key3":0.3}})_");
} // TestComplexMapAsObject_Double

Y_UNIT_TEST(TestComplexMapAsObject_Nested) {
    TestComplexMapAsObject(
        [](TComplexMapType& proto) {
            TComplexMapType inner;
            auto& innerItems = *inner.MutableString();
            innerItems["key"] = "value";
            auto& items = *proto.MutableNested();
            items["key1"] = inner;
            items["key2"] = inner;
            items["key3"] = inner;
        },
        R"_({"Nested":{"key1":{"String":{"key":"value"}},"key2":{"String":{"key":"value"}},"key3":{"String":{"key":"value"}}}})_");
} // TestComplexMapAsObject_Nested

Y_UNIT_TEST(TestMapAsObjectConfigNotSet) {
    TString modelStr(R"_({"Items":{"key":"value"}})_");

    TJson2ProtoConfig config;
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        Json2Proto<TMapType>(modelStr, config), yexception,
        "Map as object representation is not allowed");
} // TestMapAsObjectNotSet

Y_UNIT_TEST(TestMergeFlatOptional) {
    const NJson::TJsonValue& json = CreateFlatJson();

    NJson::TJsonValue patch;
    patch["I32"] = 5;
    patch["Bool"] = false;
    patch["String"] = "abacaba";
    patch["Double"] = 0.123;

    TFlatOptional proto;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto));
    UNIT_ASSERT_NO_EXCEPTION(MergeJson2Proto(patch, proto));

    TFlatRequired modelProto;
    FillFlatProto(&modelProto);
    modelProto.SetI32(5);
    modelProto.SetBool(false);
    modelProto.SetString("abacaba");
    modelProto.SetDouble(0.123);

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMergeFlatOptional

Y_UNIT_TEST(TestMergeFlatRequired) {
    const NJson::TJsonValue& json = CreateFlatJson();

    NJson::TJsonValue patch;
    patch["I32"] = 5;
    patch["Bool"] = false;
    patch["String"] = "abacaba";
    patch["Double"] = 0.123;

    TFlatRequired proto;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto));
    UNIT_ASSERT_NO_EXCEPTION(MergeJson2Proto(patch, proto));

    TFlatRequired modelProto;
    FillFlatProto(&modelProto);
    modelProto.SetI32(5);
    modelProto.SetBool(false);
    modelProto.SetString("abacaba");
    modelProto.SetDouble(0.123);

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMergeFlatRequired

Y_UNIT_TEST(TestMergeComposite) {
    const NJson::TJsonValue& json = CreateCompositeJson();

    NJson::TJsonValue patch;
    patch["Part"]["I32"] = 5;
    patch["Part"]["Bool"] = false;
    patch["Part"]["String"] = "abacaba";
    patch["Part"]["Double"] = 0.123;

    TCompositeOptional proto;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto));
    UNIT_ASSERT_NO_EXCEPTION(MergeJson2Proto(patch, proto));

    TCompositeOptional modelProto;
    FillCompositeProto(&modelProto);
    modelProto.MutablePart()->SetI32(5);
    modelProto.MutablePart()->SetBool(false);
    modelProto.MutablePart()->SetString("abacaba");
    modelProto.MutablePart()->SetDouble(0.123);

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMergeComposite

Y_UNIT_TEST(TestMergeRepeatedReplace) {
    const NJson::TJsonValue& json = CreateRepeatedFlatJson();

    NJson::TJsonValue patch;
    patch["I32"].AppendValue(5);
    patch["I32"].AppendValue(6);
    patch["String"].AppendValue("abacaba");

    TFlatRepeated proto;
    TJson2ProtoConfig config;
    config.ReplaceRepeatedFields = true;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto));
    UNIT_ASSERT_NO_EXCEPTION(MergeJson2Proto(patch, proto, config));

    TFlatRepeated modelProto;
    FillRepeatedProto(&modelProto);
    modelProto.ClearI32();
    modelProto.AddI32(5);
    modelProto.AddI32(6);
    modelProto.ClearString();
    modelProto.AddString("abacaba");

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMergeRepeatedReplace

Y_UNIT_TEST(TestMergeRepeatedAppend) {
    const NJson::TJsonValue& json = CreateRepeatedFlatJson();

    NJson::TJsonValue patch;
    patch["I32"].AppendValue(5);
    patch["I32"].AppendValue(6);
    patch["String"].AppendValue("abacaba");

    TFlatRepeated proto;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto));
    UNIT_ASSERT_NO_EXCEPTION(MergeJson2Proto(patch, proto));

    TFlatRepeated modelProto;
    FillRepeatedProto(&modelProto);
    modelProto.AddI32(5);
    modelProto.AddI32(6);
    modelProto.AddString("abacaba");

    UNIT_ASSERT_PROTOS_EQUAL(proto, modelProto);
} // TestMergeRepeatedAppend

Y_UNIT_TEST(TestEmptyStringForCastFromString) {
    NJson::TJsonValue json;
    json["I32"] = "";
    json["Bool"] = "";
    json["OneString"] = "";

    TJson2ProtoConfig config;
    config.SetCastFromString(true);
    config.SetDoNotCastEmptyStrings(true);
    TFlatOptional proto;
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto, config));
    UNIT_ASSERT(!proto.HasBool());
    UNIT_ASSERT(!proto.HasI32());
    UNIT_ASSERT(proto.HasOneString());
    UNIT_ASSERT_EQUAL("", proto.GetOneString());
} // TestEmptyStringForCastFromString

Y_UNIT_TEST(TestAllowComments) {
    constexpr TStringBuf json = R"(
{
    "I32": 4, // comment1
/*
    comment2
    {}
    qwer
*/
    "I64": 3423
}

)";

    TJson2ProtoConfig config;
    TFlatOptional proto;
    UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto(json, proto, config), yexception, "Error: Missing a name for object member");

    config.SetAllowComments(true);
    UNIT_ASSERT_NO_EXCEPTION(Json2Proto(json, proto, config));
    UNIT_ASSERT_VALUES_EQUAL(proto.GetI32(), 4);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetI64(), 3423);
    proto = TFlatOptional();
    UNIT_ASSERT_NO_EXCEPTION(proto = Json2Proto<TFlatOptional>(json, config));
    UNIT_ASSERT_VALUES_EQUAL(proto.GetI32(), 4);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetI64(), 3423);
} // TestAllowComments

Y_UNIT_TEST(TestSimplifiedDuration) {
    NJson::TJsonValue json;
    TSingleDuration simpleDuration;
    json["Duration"] = "10.1s";
    NProtobufJson::Json2Proto(json, simpleDuration, NProtobufJson::TJson2ProtoConfig().SetAllowString2TimeConversion(true));
    UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(simpleDuration.GetDuration()), TDuration::MilliSeconds(10100));
} // TestSimplifiedDuration

Y_UNIT_TEST(TestUnwrappedDuration) {
    NJson::TJsonValue json;
    TSingleDuration duration;
    json["Duration"]["seconds"] = 2;
    NProtobufJson::Json2Proto(json, duration, NProtobufJson::TJson2ProtoConfig());
    UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(duration.GetDuration()), TDuration::MilliSeconds(2000));
} // TestUnwrappedDuration

Y_UNIT_TEST(TestSimplifiedTimestamp) {
    NJson::TJsonValue json;
    TSingleTimestamp simpleTimestamp;
    json["Timestamp"] = "2014-08-26T15:52:15Z";
    NProtobufJson::Json2Proto(json, simpleTimestamp, NProtobufJson::TJson2ProtoConfig().SetAllowString2TimeConversion(true));
    UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(simpleTimestamp.GetTimestamp()), TInstant::ParseIso8601("2014-08-26T15:52:15Z"));
} // TestSimplifiedTimestamp

} // TJson2ProtoTest
