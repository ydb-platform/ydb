#include <ydb/core/protos/config.pb.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/svnversion/svnversion.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/generic/set.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

using namespace google::protobuf;

void Proto2Json(const Message& proto, TSet<TString> printedMessages, NJson::TJsonValue& json);

void PrintSingleFieldValue(const Message& proto,
                     const TSet<TString>& printedMessages,
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
            Proto2Json(reflection->GetMessage(proto, &field), printedMessages, json);
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

NJson::TJsonValue GetRepeatedFieldValue(const FieldDescriptor& field, const TSet<TString>& printedMessages) {
    switch (field.cpp_type()) {
        case FieldDescriptor::CPPTYPE_MESSAGE: {
            NJson::TJsonValue json(NJson::JSON_NULL);
            Proto2Json(*MessageFactory::generated_factory()->GetPrototype(field.message_type()), printedMessages, json);
            return json;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            return field.enum_type()->DebugString();
        }

        default:
            return NJson::JSON_NULL;
    }
}

void PrintSingleField(const Message& proto,
                     const TSet<TString>& printedMessages,
                     const FieldDescriptor& field,
                     NJson::TJsonValue& json, TStringBuf key) {
        Y_ABORT_UNLESS(!field.is_repeated(), "field is repeated.");
        json[key]["id"] = field.number();
        json[key]["file"] = field.file()->name();
        PrintSingleFieldValue(proto, printedMessages, field, json[key]["default-value"]);
}

void PrintRepeatedField(const FieldDescriptor& field, const TSet<TString>& printedMessages,
                        NJson::TJsonValue& json, TStringBuf key) {
        Y_ABORT_UNLESS(field.is_repeated(), "field isn't repeated.");
        json[key]["id"] = field.number();
        json[key]["file"] = field.file()->name();
        auto& array = json[key].InsertValue("default-value", NJson::TJsonArray());
        auto value = GetRepeatedFieldValue(field, printedMessages);
        if (!value.IsNull()) {
            NJson::TJsonValue valueMeta;
            valueMeta["id"] = field.number();
            valueMeta["file"] = field.file()->name();
            valueMeta["default-value"] = value;
            array.AppendValue(valueMeta);
        }
    }

void PrintField(const Message& proto, const TSet<TString>& printedMessages,
                const FieldDescriptor& field,
                NJson::TJsonValue& json, TStringBuf key) {
    if (field.is_repeated())
        PrintRepeatedField(field, printedMessages, json, key);
    else
        PrintSingleField(proto, printedMessages, field, json, key);
}

void Proto2Json(const Message& proto, TSet<TString> printedMessages, NJson::TJsonValue& json) {
        const auto* descriptor = proto.GetDescriptor();
        Y_ASSERT(descriptor);

        if (!printedMessages.emplace(descriptor->full_name()).second) {
            return;
        };
        // Iterate over all non-extension fields
        for (int f = 0, endF = descriptor->field_count(); f < endF; ++f) {
            const FieldDescriptor* field = descriptor->field(f);
            Y_ASSERT(field);
            PrintField(proto, printedMessages, *field, json, field->name());
        }

        // Check extensions via ListFields
        std::vector<const FieldDescriptor*> fields;
        auto* ref = proto.GetReflection();
        ref->ListFields(proto, &fields);

        for (const auto* field : fields) {
            Y_ASSERT(field);
            if (field->is_extension()) {
                PrintField(proto, printedMessages, *field, json, field->full_name());
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
    Proto2Json(defaultConf, {}, json["proto"]);
    json["branch"] = branch;
    json["commit"] = commit;
    NJson::WriteJson(&Cout, &json, true, true);
}