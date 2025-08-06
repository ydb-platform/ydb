#include <ydb/core/protos/config.pb.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/svnversion/svnversion.h>
#include <util/string/split.h>
#include <util/system/env.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

using namespace google::protobuf;

void Proto2Json(const Message& proto, NJson::TJsonValue& json);

void PrintSingleFieldValue(const Message& proto,
                     const FieldDescriptor& field,
                     NJson::TJsonValue& json) {
    #define FIELD_TO_JSON(EProtoCppType, ProtoGet)                         \
        case FieldDescriptor::EProtoCppType: {                             \
            json = reflection->ProtoGet(proto, &field); \
            break;                                                         \
        }

    const Reflection* reflection = proto.GetReflection();
    switch (field.cpp_type()) {
        FIELD_TO_JSON(CPPTYPE_INT32, GetInt32);
        FIELD_TO_JSON(CPPTYPE_INT64, GetInt64);
        FIELD_TO_JSON(CPPTYPE_UINT32, GetUInt32);
        FIELD_TO_JSON(CPPTYPE_UINT64, GetUInt64);
        FIELD_TO_JSON(CPPTYPE_DOUBLE, GetDouble);
        FIELD_TO_JSON(CPPTYPE_FLOAT, GetFloat);
        FIELD_TO_JSON(CPPTYPE_BOOL, GetBool);
        FIELD_TO_JSON(CPPTYPE_STRING, GetString);

        case FieldDescriptor::CPPTYPE_MESSAGE: {
            json.SetType(NJson::JSON_MAP);
            Proto2Json(reflection->GetMessage(proto, &field), json);
            break;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            json = reflection->GetEnum(proto, &field)->name();
            break;
        }

        default:
            ythrow yexception() << "Unknown protobuf field type: "
                                << static_cast<int>(field.cpp_type()) << ".";
    }
    #undef FIELD_TO_JSON
}

void PrintRepeatedFieldValue(const Message& proto,
                     const FieldDescriptor& field,
                     NJson::TJsonValue& json, size_t index) {
    #define FIELD_TO_JSON(EProtoCppType, ProtoGet)                         \
        case FieldDescriptor::EProtoCppType: {                             \
            json = reflection->ProtoGet(proto, &field, index); \
            break;                                                         \
        }

    const Reflection* reflection = proto.GetReflection();
    switch (field.cpp_type()) {
        FIELD_TO_JSON(CPPTYPE_INT32, GetRepeatedInt32);
        FIELD_TO_JSON(CPPTYPE_INT64, GetRepeatedInt64);
        FIELD_TO_JSON(CPPTYPE_UINT32, GetRepeatedUInt32);
        FIELD_TO_JSON(CPPTYPE_UINT64, GetRepeatedUInt64);
        FIELD_TO_JSON(CPPTYPE_DOUBLE, GetRepeatedDouble);
        FIELD_TO_JSON(CPPTYPE_FLOAT, GetRepeatedFloat);
        FIELD_TO_JSON(CPPTYPE_BOOL, GetRepeatedBool);
        FIELD_TO_JSON(CPPTYPE_STRING, GetRepeatedString);

        case FieldDescriptor::CPPTYPE_MESSAGE: {
            json.SetType(NJson::JSON_MAP);
            Proto2Json(reflection->GetRepeatedMessage(proto, &field, index), json);
            break;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            json = reflection->GetRepeatedEnum(proto, &field, index)->name();
            break;
        }

        default:
            ythrow yexception() << "Unknown protobuf field type: "
                                << static_cast<int>(field.cpp_type()) << ".";
    }
    #undef FIELD_TO_JSON
}

void PrintSingleField(const Message& proto,
                     const FieldDescriptor& field,
                     NJson::TJsonValue& json, TStringBuf key) {
        Y_ABORT_UNLESS(!field.is_repeated(), "field is repeated.");
        json[key]["id"] = field.number();
        PrintSingleFieldValue(proto, field, json[key]["default-value"]);
}

    void PrintRepeatedField(const Message& proto,
                                                const FieldDescriptor& field,
                                                NJson::TJsonValue& json, TStringBuf key) {
        Y_ABORT_UNLESS(field.is_repeated(), "field isn't repeated.");
        const Reflection* reflection = proto.GetReflection();
        json[key]["id"] = field.number();
        auto& array = json[key].InsertValue("default-value", NJson::TJsonArray());
        for (size_t i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i) {
            PrintRepeatedFieldValue(proto, field, array.AppendValue(NJson::JSON_UNDEFINED), i);
        }
    }

void PrintField(const Message& proto,
                const FieldDescriptor& field,
                NJson::TJsonValue& json, TStringBuf key) {
    if (field.is_repeated())
        PrintRepeatedField(proto, field, json, key);
    else
        PrintSingleField(proto, field, json, key);
}

void Proto2Json(const Message& proto, NJson::TJsonValue& json) {
        const auto* descriptor = proto.GetDescriptor();
        Y_ASSERT(descriptor);

        // Iterate over all non-extension fields
        for (int f = 0, endF = descriptor->field_count(); f < endF; ++f) {
            const FieldDescriptor* field = descriptor->field(f);
            Y_ASSERT(field);
            PrintField(proto, *field, json, field->name());
        }

        // Check extensions via ListFields
        std::vector<const FieldDescriptor*> fields;
        auto* ref = proto.GetReflection();
        ref->ListFields(proto, &fields);

        for (const auto* field : fields) {
            Y_ASSERT(field);
            if (field->is_extension()) {
                PrintField(proto, *field, json, field->full_name());
            }
        }

}

int main(int argc, const char** argv) {
    NLastGetopt::TOpts opts;
    auto branch = StringSplitter(GetBranch()).Split('/').ToList<TString>().back();
    auto commit = GetProgramCommitId();

    opts.AddLongOption("override-branch").StoreResult(&branch);
    opts.AddLongOption("override-commit").StoreResult(&commit);
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);
    const auto defaultConf = NKikimrConfig::TAppConfig::default_instance();
    NJson::TJsonValue json;
    Proto2Json(defaultConf, json["proto"]);
    json["branch"] = branch;
    json["commit"] = commit;
    NJson::WriteJson(&Cout, &json, true, true);
}