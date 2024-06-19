#include "json.h"
#include "proto.h"

#include <library/cpp/protobuf/json/ut/test.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/protobuf/json/proto2json.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/ylimits.h>

#include <util/stream/str.h>

#include <util/system/defaults.h>
#include <util/system/yassert.h>

#include <limits>

using namespace NProtobufJson;
using namespace NProtobufJsonTest;

Y_UNIT_TEST_SUITE(TProto2JsonFlatTest) {
    Y_UNIT_TEST(TestFlatDefault) {
        using namespace ::google::protobuf;
        TFlatDefault proto;
        NJson::TJsonValue json;
        TProto2JsonConfig cfg;
        cfg.SetMissingSingleKeyMode(TProto2JsonConfig::MissingKeyDefault);
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, cfg));
#define DEFINE_FIELD(name, value)                                                                     \
    {                                                                                                 \
        auto descr = proto.GetMetadata().descriptor->FindFieldByName(#name);                          \
        UNIT_ASSERT(descr);                                                                           \
        UNIT_ASSERT(json.Has(#name));                                                                 \
        switch (descr->cpp_type()) {                                                                  \
            case FieldDescriptor::CPPTYPE_INT32:                                                      \
                UNIT_ASSERT(descr->default_value_int32() == json[#name].GetIntegerRobust());          \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_INT64:                                                      \
                UNIT_ASSERT(descr->default_value_int64() == json[#name].GetIntegerRobust());          \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_UINT32:                                                     \
                UNIT_ASSERT(descr->default_value_uint32() == json[#name].GetUIntegerRobust());        \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_UINT64:                                                     \
                UNIT_ASSERT(descr->default_value_uint32() == json[#name].GetUIntegerRobust());        \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_DOUBLE:                                                     \
                UNIT_ASSERT(descr->default_value_double() == json[#name].GetDoubleRobust());          \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_FLOAT:                                                      \
                UNIT_ASSERT(descr->default_value_float() == json[#name].GetDoubleRobust());           \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_BOOL:                                                       \
                UNIT_ASSERT(descr->default_value_bool() == json[#name].GetBooleanRobust());           \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_ENUM:                                                       \
                UNIT_ASSERT(descr->default_value_enum()->number() == json[#name].GetIntegerRobust()); \
                break;                                                                                \
            case FieldDescriptor::CPPTYPE_STRING:                                                     \
                UNIT_ASSERT(descr->default_value_string() == json[#name].GetStringRobust());          \
                break;                                                                                \
            default:                                                                                  \
                break;                                                                                \
        }                                                                                             \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
    }

    Y_UNIT_TEST(TestOneOfDefault) {
        using namespace ::google::protobuf;
        TFlatOneOfDefault proto;
        NJson::TJsonValue json;
        TProto2JsonConfig cfg;
        cfg.SetMissingSingleKeyMode(TProto2JsonConfig::MissingKeyDefault);
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, cfg));

        UNIT_ASSERT(!json.Has("ChoiceOne"));
        UNIT_ASSERT(!json.Has("ChoiceTwo"));

        proto.SetChoiceOne("one");
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, cfg));
        UNIT_ASSERT_EQUAL("one", json["ChoiceOne"].GetStringRobust());
    }

    Y_UNIT_TEST(TestNameGenerator) {
        TNameGeneratorType proto;
        proto.SetField(42);

        TProto2JsonConfig cfg;
        cfg.SetNameGenerator([](const NProtoBuf::FieldDescriptor&) { return "42"; });

        TStringStream str;
        Proto2Json(proto, str, cfg);

        UNIT_ASSERT_STRINGS_EQUAL(R"({"42":42})", str.Str());
    }

    Y_UNIT_TEST(TestEnumValueGenerator) {
        TEnumValueGeneratorType proto;
        proto.SetEnum(TEnumValueGeneratorType::ENUM_42);

        TProto2JsonConfig cfg;
        cfg.SetEnumValueGenerator([](const NProtoBuf::EnumValueDescriptor&) { return "42"; });

        TStringStream str;
        Proto2Json(proto, str, cfg);

        UNIT_ASSERT_STRINGS_EQUAL(R"({"Enum":"42"})", str.Str());
    }

    Y_UNIT_TEST(TestFlatOptional){
        {TFlatOptional proto;
    FillFlatProto(&proto);
    const NJson::TJsonValue& modelJson = CreateFlatJson();
    {
        NJson::TJsonValue json;
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        TStringStream jsonStream;
        NJson::TJsonValue json;
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
        UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    } // streamed
}

    // Try to skip each field
#define DEFINE_FIELD(name, value)                                          \
    {                                                                      \
        THashSet<TString> skippedField;                                    \
        skippedField.insert(#name);                                        \
        TFlatOptional proto;                                               \
        FillFlatProto(&proto, skippedField);                               \
        const NJson::TJsonValue& modelJson = CreateFlatJson(skippedField); \
        NJson::TJsonValue json;                                            \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));                 \
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                          \
        {                                                                  \
            TStringStream jsonStream;                                      \
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));       \
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));                 \
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                      \
        }                                                                  \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestFlatOptional

Y_UNIT_TEST(TestFlatRequired){
    {TFlatRequired proto;
FillFlatProto(&proto);
const NJson::TJsonValue& modelJson = CreateFlatJson();
{
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
}

{
    TStringStream jsonStream;
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
    UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
} // streamed
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                          \
    {                                                                      \
        THashSet<TString> skippedField;                                    \
        skippedField.insert(#name);                                        \
        TFlatRequired proto;                                               \
        FillFlatProto(&proto, skippedField);                               \
        const NJson::TJsonValue& modelJson = CreateFlatJson(skippedField); \
        NJson::TJsonValue json;                                            \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));                 \
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                          \
        {                                                                  \
            TStringStream jsonStream;                                      \
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));       \
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));                 \
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                      \
        }                                                                  \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestFlatRequired

Y_UNIT_TEST(TestFlatRepeated) {
    {
        TFlatRepeated proto;
        FillRepeatedProto(&proto);
        const NJson::TJsonValue& modelJson = CreateRepeatedFlatJson();
        {
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        }

        {
            TStringStream jsonStream;
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        } // streamed
    }

    TProto2JsonConfig config;
    config.SetMissingRepeatedKeyMode(TProto2JsonConfig::MissingKeySkip);

    // Try to skip each field
#define DEFINE_REPEATED_FIELD(name, ...)                                           \
    {                                                                              \
        THashSet<TString> skippedField;                                            \
        skippedField.insert(#name);                                                \
        TFlatRepeated proto;                                                       \
        FillRepeatedProto(&proto, skippedField);                                   \
        const NJson::TJsonValue& modelJson = CreateRepeatedFlatJson(skippedField); \
        NJson::TJsonValue json;                                                    \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));                 \
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                                  \
        {                                                                          \
            TStringStream jsonStream;                                              \
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream, config));       \
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));                         \
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                              \
        }                                                                          \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD
} // TestFlatRepeated

Y_UNIT_TEST(TestCompositeOptional){
    {TCompositeOptional proto;
FillCompositeProto(&proto);
const NJson::TJsonValue& modelJson = CreateCompositeJson();
{
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
}

{
    TStringStream jsonStream;
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
    UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
} // streamed
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                               \
    {                                                                           \
        THashSet<TString> skippedField;                                         \
        skippedField.insert(#name);                                             \
        TCompositeOptional proto;                                               \
        FillCompositeProto(&proto, skippedField);                               \
        const NJson::TJsonValue& modelJson = CreateCompositeJson(skippedField); \
        NJson::TJsonValue json;                                                 \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));                      \
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                               \
        {                                                                       \
            TStringStream jsonStream;                                           \
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));            \
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));                      \
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                           \
        }                                                                       \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestCompositeOptional

Y_UNIT_TEST(TestCompositeRequired){
    {TCompositeRequired proto;
FillCompositeProto(&proto);
const NJson::TJsonValue& modelJson = CreateCompositeJson();
{
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
}

{
    TStringStream jsonStream;
    NJson::TJsonValue json;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
    UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
    UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
} // streamed
}

// Try to skip each field
#define DEFINE_FIELD(name, value)                                               \
    {                                                                           \
        THashSet<TString> skippedField;                                         \
        skippedField.insert(#name);                                             \
        TCompositeRequired proto;                                               \
        FillCompositeProto(&proto, skippedField);                               \
        const NJson::TJsonValue& modelJson = CreateCompositeJson(skippedField); \
        NJson::TJsonValue json;                                                 \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));                      \
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                               \
        {                                                                       \
            TStringStream jsonStream;                                           \
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));            \
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));                      \
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);                           \
        }                                                                       \
    }
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD
} // TestCompositeRequired

Y_UNIT_TEST(TestCompositeRepeated) {
    {
        TFlatOptional partProto;
        FillFlatProto(&partProto);
        TCompositeRepeated proto;
        proto.AddPart()->CopyFrom(partProto);

        NJson::TJsonValue modelJson;
        NJson::TJsonValue modelArray;
        modelArray.AppendValue(CreateFlatJson());
        modelJson.InsertValue("Part", modelArray);
        {
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        }

        {
            TStringStream jsonStream;
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        } // streamed
    }

    {
        // Array of messages with each field skipped
        TCompositeRepeated proto;
        NJson::TJsonValue modelArray;

#define DEFINE_REPEATED_FIELD(name, ...)                      \
    {                                                         \
        THashSet<TString> skippedField;                       \
        skippedField.insert(#name);                           \
        TFlatOptional partProto;                              \
        FillFlatProto(&partProto, skippedField);              \
        proto.AddPart()->CopyFrom(partProto);                 \
        modelArray.AppendValue(CreateFlatJson(skippedField)); \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD

        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Part", modelArray);

        {
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        }

        {
            TStringStream jsonStream;
            NJson::TJsonValue json;
            UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStream));
            UNIT_ASSERT(ReadJsonTree(&jsonStream, &json));
            UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
        } // streamed
    }
} // TestCompositeRepeated

Y_UNIT_TEST(TestEnumConfig) {
    {
        TFlatOptional proto;
        proto.SetEnum(E_1);
        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Enum", 1);
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.EnumMode = TProto2JsonConfig::EnumNumber;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        TFlatOptional proto;
        proto.SetEnum(E_1);
        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Enum", "E_1");
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.EnumMode = TProto2JsonConfig::EnumName;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        TFlatOptional proto;
        proto.SetEnum(E_1);
        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Enum", "NProtobufJsonTest.E_1");
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.EnumMode = TProto2JsonConfig::EnumFullName;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        TFlatOptional proto;
        proto.SetEnum(E_1);
        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Enum", "e_1");
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.EnumMode = TProto2JsonConfig::EnumNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        TFlatOptional proto;
        proto.SetEnum(E_1);
        NJson::TJsonValue modelJson;
        modelJson.InsertValue("Enum", "nprotobufjsontest.e_1");
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.EnumMode = TProto2JsonConfig::EnumFullNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
} // TestEnumConfig

Y_UNIT_TEST(TestMissingSingleKeyConfig) {
    {
        TFlatOptional proto;
        NJson::TJsonValue modelJson(NJson::JSON_MAP);
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeySkip;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        NJson::TJsonValue modelJson;
#define DEFINE_FIELD(name, value) \
    modelJson.InsertValue(#name, NJson::TJsonValue(NJson::JSON_NULL));
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD

        TFlatOptional proto;
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeyNull;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
    {
        // Test MissingKeyExplicitDefaultThrowRequired for non explicit default values.
        TFlatOptional proto;
        NJson::TJsonValue modelJson(NJson::JSON_MAP);
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
    {
        // Test MissingKeyExplicitDefaultThrowRequired for explicit default values.
        NJson::TJsonValue modelJson;
        modelJson["String"] = "value";

        TSingleDefaultString proto;
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
    {
        // Test MissingKeyExplicitDefaultThrowRequired for empty required values.
        TFlatRequired proto;
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;
        UNIT_ASSERT_EXCEPTION_CONTAINS(Proto2Json(proto, json, config), yexception, "Empty required protobuf field");
    }
    {
        // Test MissingKeyExplicitDefaultThrowRequired for required value.
        TSingleRequiredString proto;
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingSingleKeyMode = TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;

        UNIT_ASSERT_EXCEPTION_CONTAINS(Proto2Json(proto, json, config), yexception, "Empty required protobuf field");

        NJson::TJsonValue modelJson;
        modelJson["String"] = "value";
        proto.SetString("value");
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
} // TestMissingSingleKeyConfig

Y_UNIT_TEST(TestMissingRepeatedKeyNoConfig) {
    {
        TFlatRepeated proto;
        NJson::TJsonValue modelJson(NJson::JSON_MAP);
        NJson::TJsonValue json;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
} // TestMissingRepeatedKeyNoConfig

Y_UNIT_TEST(TestMissingRepeatedKeyConfig) {
    {
        TFlatRepeated proto;
        NJson::TJsonValue modelJson(NJson::JSON_MAP);
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingRepeatedKeyMode = TProto2JsonConfig::MissingKeySkip;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }

    {
        NJson::TJsonValue modelJson;
#define DEFINE_FIELD(name, value) \
    modelJson.InsertValue(#name, NJson::TJsonValue(NJson::JSON_NULL));
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD

        TFlatRepeated proto;
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingRepeatedKeyMode = TProto2JsonConfig::MissingKeyNull;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
    {
        TFlatRepeated proto;
        NJson::TJsonValue modelJson(NJson::JSON_MAP);
        NJson::TJsonValue json;
        TProto2JsonConfig config;
        config.MissingRepeatedKeyMode = TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired;

        // SHould be same as MissingKeySkip
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, json, config));
        UNIT_ASSERT_JSONS_EQUAL(json, modelJson);
    }
} // TestMissingRepeatedKeyConfig

Y_UNIT_TEST(TestEscaping) {
    // No escape
    {
        TString modelStr(R"_({"String":"value\""})_");

        TFlatOptional proto;
        proto.SetString(R"_(value")_");
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // TEscapeJTransform
    {
        TString modelStr(R"_({"String":"value\""})_");

        TFlatOptional proto;
        proto.SetString(R"_(value")_");
        TProto2JsonConfig config;
        config.StringTransforms.push_back(new TEscapeJTransform<false, true>());
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(modelStr, jsonStr.Str());
    }

    // TCEscapeTransform
    {
        TString modelStr(R"_({"String":"value\""})_");

        TFlatOptional proto;
        proto.SetString(R"_(value")_");
        TProto2JsonConfig config;
        config.StringTransforms.push_back(new TCEscapeTransform());
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // TSafeUtf8CEscapeTransform
    {
        TString modelStr(R"_({"String":"value\""})_");

        TFlatOptional proto;
        proto.SetString(R"_(value")_");
        TProto2JsonConfig config;
        config.StringTransforms.push_back(new TSafeUtf8CEscapeTransform());
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
} // TestEscaping

class TBytesTransform: public IStringTransform {
public:
    int GetType() const override {
        return 0;
    }
    void Transform(TString&) const override {
    }
    void TransformBytes(TString& str) const override {
        str = "bytes";
    }
};

Y_UNIT_TEST(TestBytesTransform) {
    // Test that string field is not changed
    {
        TString modelStr(R"_({"String":"value"})_");

        TFlatOptional proto;
        proto.SetString(R"_(value)_");
        TProto2JsonConfig config;
        config.StringTransforms.push_back(new TBytesTransform());
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Test that bytes field is changed
    {
        TString modelStr(R"_({"Bytes":"bytes"})_");

        TFlatOptional proto;
        proto.SetBytes(R"_(value)_");
        TProto2JsonConfig config;
        config.StringTransforms.push_back(new TBytesTransform());
        TStringStream jsonStr;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
}

Y_UNIT_TEST(TestFieldNameMode) {
    // Original case 1
    {
        TString modelStr(R"_({"String":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Original case 2
    {
        TString modelStr(R"_({"String":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameOriginalCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Lowercase
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Uppercase
    {
        TString modelStr(R"_({"STRING":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameUpperCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Camelcase
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
    {
        TString modelStr(R"_({"oneString":"value"})_");

        TFlatOptional proto;
        proto.SetOneString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
    {
        TString modelStr(R"_({"oneTwoString":"value"})_");

        TFlatOptional proto;
        proto.SetOneTwoString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameCamelCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // snake_case
    {
        TString modelStr(R"_({"string":"value"})_");

        TFlatOptional proto;
        proto.SetString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
    {
        TString modelStr(R"_({"one_string":"value"})_");

        TFlatOptional proto;
        proto.SetOneString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
    {
        TString modelStr(R"_({"one_two_string":"value"})_");

        TFlatOptional proto;
        proto.SetOneTwoString("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }
    {
        TString modelStr(R"_({"a_b_c":"value","user_i_d":"value"})_");

        TFlatOptional proto;
        proto.SetABC("value");
        proto.SetUserID("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameSnakeCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // snake_case_dense
    {
        TString modelStr(R"_({"abc":"value","user_id":"value"})_");

        TFlatOptional proto;
        proto.SetABC("value");
        proto.SetUserID("value");
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameSnakeCaseDense;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Original case, repeated
    {
        TString modelStr(R"_({"I32":[1,2]})_");

        TFlatRepeated proto;
        proto.AddI32(1);
        proto.AddI32(2);
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameOriginalCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // Lower case, repeated
    {
        TString modelStr(R"_({"i32":[1,2]})_");

        TFlatRepeated proto;
        proto.AddI32(1);
        proto.AddI32(2);
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.FieldNameMode = TProto2JsonConfig::FieldNameLowerCase;

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // UseJsonName
    {
        // FIXME(CONTRIB-139): see the comment about UseJsonName in json2proto_ut.cpp:
        // Def_upper json name should be "DefUpper".
        TString modelStr(R"_({"My-Upper":1,"my-lower":2,"defUpper":3,"defLower":4})_");

        TWithJsonName proto;
        proto.Setmy_upper(1);
        proto.SetMy_lower(2);
        proto.SetDef_upper(3);
        proto.Setdef_lower(4);
        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.SetUseJsonName(true);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // UseJsonName and UseJsonEnumValue
    {
        TString modelStr(R"_({"json_enum":"enum_1"})_");

        TCustomJsonEnumValue proto;
        proto.SetJsonEnum(EJsonEnum::J_1);

        TStringStream jsonStr;
        TProto2JsonConfig config;
        config.SetUseJsonName(true);
        config.SetUseJsonEnumValue(true);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
        UNIT_ASSERT_STRINGS_EQUAL(jsonStr.Str(), modelStr);
    }

    // FieldNameMode with UseJsonName
    {
        TProto2JsonConfig config;
        config.SetFieldNameMode(TProto2JsonConfig::FieldNameLowerCase);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.SetUseJsonName(true), yexception, "mutually exclusive");
    }
    {
        TProto2JsonConfig config;
        config.SetUseJsonName(true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.SetFieldNameMode(TProto2JsonConfig::FieldNameLowerCase), yexception, "mutually exclusive");
    }

    /// TODO: test missing keys
} // TestFieldNameMode

Y_UNIT_TEST(TestNan) {
    TFlatOptional proto;
    proto.SetDouble(std::numeric_limits<double>::quiet_NaN());

    UNIT_ASSERT_EXCEPTION(Proto2Json(proto, TProto2JsonConfig()), yexception);
} // TestNan

Y_UNIT_TEST(TestInf) {
    TFlatOptional proto;
    proto.SetFloat(std::numeric_limits<float>::infinity());

    UNIT_ASSERT_EXCEPTION(Proto2Json(proto, TProto2JsonConfig()), yexception);
} // TestInf

Y_UNIT_TEST(TestMap) {
    TMapType proto;

    auto& items = *proto.MutableItems();
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    TString modelStr(R"_({"Items":[{"key":"key3","value":"value3"},{"key":"key2","value":"value2"},{"key":"key1","value":"value1"}]})_");

    TStringStream jsonStr;
    TProto2JsonConfig config;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));

    NJson::TJsonValue jsonValue, modelValue;
    NJson::TJsonValue::TArray jsonItems, modelItems;
    UNIT_ASSERT(NJson::ReadJsonTree(jsonStr.Str(), &jsonValue));
    UNIT_ASSERT(NJson::ReadJsonTree(modelStr, &modelValue));
    UNIT_ASSERT(jsonValue.Has("Items"));
    jsonValue["Items"].GetArray(&jsonItems);
    modelValue["Items"].GetArray(&modelItems);
    auto itemKey = [](const NJson::TJsonValue& v) {
        return v["key"].GetString();
    };
    SortBy(jsonItems, itemKey);
    SortBy(modelItems, itemKey);
    UNIT_ASSERT_EQUAL(jsonItems, modelItems);
} // TestMap

Y_UNIT_TEST(TestMapAsObject) {
    TMapType proto;

    auto& items = *proto.MutableItems();
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    TString modelStr(R"_({"Items":{"key3":"value3","key2":"value2","key1":"value1"}})_");

    TStringStream jsonStr;
    TProto2JsonConfig config;
    config.MapAsObject = true;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));

    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
} // TestMapAsObject

Y_UNIT_TEST(TestMapUsingGeneratedAsJSON) {
    TMapType proto;

    auto& items = *proto.MutableItems();
    items["key1"] = "value1";
    items["key2"] = "value2";
    items["key3"] = "value3";

    TString modelStr(R"_({"Items":{"key3":"value3","key2":"value2","key1":"value1"}})_");

    TStringStream jsonStr;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr));

    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
} // TestMapUsingGeneratedAsJSON

Y_UNIT_TEST(TestMapDefaultValue) {
    TMapType proto;

    auto& items = *proto.MutableItems();
    items["key1"] = "";

    TString modelStr(R"_({"Items":{"key1":""}})_");

    TStringStream jsonStr;

    TProto2JsonConfig config;
    config.MapAsObject = true;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);

    jsonStr.Clear();
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr));
    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
} // TestMapDefaultValue

Y_UNIT_TEST(TestMapDefaultMessageValue) {
    TComplexMapType proto;

    auto& map = *proto.MutableNested();
    map["key1"];  // Creates an empty nested message

    TString modelStr(R"_({"Nested":{"key1":{}}})_");

    TStringStream jsonStr;

    TProto2JsonConfig config;
    config.MapAsObject = true;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));
    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);

    jsonStr.Clear();
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr));
    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
} // TestMapDefaultMessageValue

Y_UNIT_TEST(TestStringifyNumbers) {
#define TEST_SINGLE(flag, field, value, expectString)                            \
    do {                                                                         \
        TFlatOptional proto;                                                     \
        proto.Set##field(value);                                                 \
                                                                                 \
        TStringStream jsonStr;                                                   \
        TProto2JsonConfig config;                                                \
        config.SetStringifyNumbers(flag);                                        \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));            \
        if (expectString) {                                                      \
            UNIT_ASSERT_EQUAL(jsonStr.Str(), "{\"" #field "\":\"" #value "\"}"); \
        } else {                                                                 \
            UNIT_ASSERT_EQUAL(jsonStr.Str(), "{\"" #field "\":" #value "}");     \
        }                                                                        \
    } while (false)

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 10000000000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -10000000000000000, false);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -10000000000000000, true);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 1000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -1000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -10000000000000000, true);


    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 1, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI32, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI64, -1, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI32, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI64, -10000000000000000, true);

#undef TEST_SINGLE
} // TestStringifyNumbers

Y_UNIT_TEST(TestStringifyNumbersRepeated) {
#define TEST_SINGLE(flag, field, value)                                          \
    do {                                                                         \
        TFlatRepeated proto;                                                     \
        proto.Add##field(value);                                                 \
                                                                                 \
        TStringStream jsonStr;                                                   \
        TProto2JsonConfig config;                                                \
        config.SetStringifyNumbers(flag);                                        \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));            \
        UNIT_ASSERT_EQUAL(jsonStr.Str(), "{\"" #field "\":[" #value "]}");       \
    } while (false)

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 1);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 10000000000000000);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 1);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 10000000000000000);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 1);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 10000000000000000);

    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 1);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 10000000000000000);

#undef TEST_SINGLE
} // TestStringifyNumbersRepeated

Y_UNIT_TEST(TestStringifyNumbersRepeatedStringification){
#define TEST_SINGLE(flag, field, value, expectString)                              \
    do {                                                                           \
        TFlatRepeated proto;                                                       \
        proto.Add##field(value);                                                   \
                                                                                   \
        TStringStream jsonStr;                                                     \
        TProto2JsonConfig config;                                                  \
        config.SetStringifyNumbersRepeated(flag);                                  \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));              \
        if (expectString) {                                                        \
            UNIT_ASSERT_EQUAL(jsonStr.Str(), "{\"" #field "\":[\"" #value "\"]}"); \
        } else {                                                                   \
            UNIT_ASSERT_EQUAL(jsonStr.Str(), "{\"" #field "\":[" #value "]}");     \
        }                                                                          \
    } while (false)

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, 10000000000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersNever, SI64, -10000000000000000, false);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForDouble, SI64, -10000000000000000, true);

    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 1000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -1, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -1000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyLongNumbersForFloat, SI64, -10000000000000000, true);


    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 1, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI32, 1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, UI64, 10000000000000000, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI64, -1, true);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI32, -1000000000, false);
    TEST_SINGLE(TProto2JsonConfig::StringifyInt64Always, SI64, -10000000000000000, true);

#undef TEST_SINGLE
} // TestStringifyNumbersRepeatedStringification

Y_UNIT_TEST(TestStringifyNumbersRepeatedStringificationList){
    using NJson::JSON_STRING;
    using NJson::JSON_UINTEGER;
    using NJson::JSON_INTEGER;

    TFlatRepeated proto;
    proto.AddUI64(1);
    proto.AddUI64(1000000000);
    proto.AddUI64(10000000000000000);
    proto.AddSI64(1);
    proto.AddSI64(1000000000);
    proto.AddSI64(10000000000000000);
    proto.AddSI64(-1);
    proto.AddSI64(-1000000000);
    proto.AddSI64(-10000000000000000);
    proto.AddUI32(1);
    proto.AddUI32(1000000000);
    proto.AddSI32(-1);
    proto.AddSI32(-1000000000);

    TProto2JsonConfig config;
    NJson::TJsonValue jsonValue;
    THashMap<TString, NJson::TJsonValue> jsonMap;
    {
        jsonValue = NJson::TJsonValue{};
        config.SetStringifyNumbersRepeated(TProto2JsonConfig::StringifyLongNumbersNever);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonValue, config));
        jsonMap = jsonValue.GetMap();

        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[1].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[2].GetType(), JSON_UINTEGER);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[1].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[2].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[3].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[4].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[5].GetType(), JSON_INTEGER);
    }
    {
        jsonValue = NJson::TJsonValue{};
        config.SetStringifyNumbersRepeated(TProto2JsonConfig::StringifyLongNumbersForDouble);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonValue, config));
        jsonMap = jsonValue.GetMap();

        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[1].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[2].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[1].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[2].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[3].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[4].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[5].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[1].GetType(), JSON_INTEGER);

        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[1].GetType(), JSON_UINTEGER);
    }
    {
        jsonValue = NJson::TJsonValue{};
        config.SetStringifyNumbersRepeated(TProto2JsonConfig::StringifyLongNumbersForFloat);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonValue, config));
        jsonMap = jsonValue.GetMap();

        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[1].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[2].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[1].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[2].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[3].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[4].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[5].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[1].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[1].GetType(), JSON_STRING);
    }
    {
        jsonValue = NJson::TJsonValue{};
        config.SetStringifyNumbersRepeated(TProto2JsonConfig::StringifyInt64Always);

        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonValue, config));
        jsonMap = jsonValue.GetMap();

        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[0].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[1].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI64")[2].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[0].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[1].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[2].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[3].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[4].GetType(), JSON_STRING);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI64")[5].GetType(), JSON_STRING);

        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[0].GetType(), JSON_INTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("SI32")[1].GetType(), JSON_INTEGER);

        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[0].GetType(), JSON_UINTEGER);
        UNIT_ASSERT_EQUAL(jsonMap.at("UI32")[1].GetType(), JSON_UINTEGER);
    }

} // TestStringifyNumbersRepeatedStringificationList

Y_UNIT_TEST(TestExtension) {
    TExtensionField proto;
    proto.SetExtension(bar, 1);

    Y_ASSERT(proto.HasExtension(bar));
    UNIT_ASSERT_EQUAL(Proto2Json(proto, TProto2JsonConfig()), "{\"NProtobufJsonTest.bar\":1}");


    TProto2JsonConfig cfg;
    cfg.SetExtensionFieldNameMode(TProto2JsonConfig::ExtFldNameShort);
    UNIT_ASSERT_EQUAL(Proto2Json(proto, cfg), "{\"bar\":1}");
} // TestExtension

Y_UNIT_TEST(TestSimplifiedDuration) {
    TString json;
    TSingleDuration simpleDuration;
    simpleDuration.mutable_duration()->set_seconds(10);
    simpleDuration.mutable_duration()->set_nanos(101);
    Proto2Json(simpleDuration, json, TProto2JsonConfig().SetConvertTimeAsString(true));
    UNIT_ASSERT_EQUAL_C(json, "{\"Duration\":\"10.000000101s\"}", "real value is " << json);
} // TestSimplifiedDuration

Y_UNIT_TEST(TestSimplifiedTimestamp) {
    TString json;
    TSingleTimestamp simpleTimestamp;
    simpleTimestamp.mutable_timestamp()->set_seconds(10000000);
    simpleTimestamp.mutable_timestamp()->set_nanos(504);
    Proto2Json(simpleTimestamp, json, TProto2JsonConfig().SetConvertTimeAsString(true));
    UNIT_ASSERT_EQUAL_C(json, "{\"Timestamp\":\"1970-04-26T17:46:40.000000504Z\"}", "real value is " << json);
} // TestSimplifiedTimestamp

Y_UNIT_TEST(TestFloatToString) {
#define TEST_SINGLE(mode, value, expectedValue)                                             \
    do {                                                                                    \
        TFlatOptional proto;                                                                \
        proto.SetFloat(value);                                                              \
                                                                                            \
        TStringStream jsonStr;                                                              \
        TProto2JsonConfig config;                                                           \
        config.SetFloatNDigits(3).SetFloatToStringMode(mode);                               \
        UNIT_ASSERT_NO_EXCEPTION(Proto2Json(proto, jsonStr, config));                       \
        TString expectedStr = TStringBuilder() << "{\"Float\":" << expectedValue << "}";    \
        UNIT_ASSERT_EQUAL_C(jsonStr.Str(), expectedStr, "real value is " << jsonStr.Str()); \
    } while (false)

    TEST_SINGLE(EFloatToStringMode::PREC_NDIGITS, 1234.18345, "1.23e+03");
    TEST_SINGLE(EFloatToStringMode::PREC_NDIGITS, 12.18345, "12.2");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS, 12345.18355, "12345.184");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS, 12.18355, "12.184");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS, 12.18, "12.180");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS_STRIP_ZEROES, 12345.18355, "12345.184");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS_STRIP_ZEROES, 12.18355, "12.184");
    TEST_SINGLE(EFloatToStringMode::PREC_POINT_DIGITS_STRIP_ZEROES, 12.18, "12.18");

#undef TEST_SINGLE
} // TestFloatToString

Y_UNIT_TEST(TestAny) {
    TProto2JsonConfig config;
    config.SetConvertAny(true);

    TString modelStr(R"_({"Any":{"@type":"type.googleapis.com/NProtobufJsonTest.TFlatOptional","String":"value\""}})_");

    TFlatOptional proto;
    proto.SetString(R"_(value")_");
    TContainsAny protoWithAny;
    protoWithAny.MutableAny()->PackFrom(proto);

    TStringStream jsonStr;
    UNIT_ASSERT_NO_EXCEPTION(Proto2Json(protoWithAny, jsonStr, config));
    UNIT_ASSERT_JSON_STRINGS_EQUAL(jsonStr.Str(), modelStr);
}

} // TProto2JsonTest
