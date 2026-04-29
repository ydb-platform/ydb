#include "security_json_printer.h"
#include "security_printer.h"

#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>

#include <google/protobuf/reflection.h>

#include <util/generic/strbuf.h>

namespace NKikimr {
namespace {

TStringBuf SensitiveJsonPlaceholder() {
    return TStringBuf("***");
}

class TSecurityProto2JsonPrinter : public NProtobufJson::TProto2JsonPrinter {
public:
    explicit TSecurityProto2JsonPrinter(const NProtobufJson::TProto2JsonConfig& config)
        : NProtobufJson::TProto2JsonPrinter(config)
    {
    }

protected:
    void PrintField(const NProtoBuf::Message& proto,
                    const NProtoBuf::FieldDescriptor& field,
                    NProtobufJson::IJsonOutput& json,
                    TStringBuf key) override {
        if (TSecurityTextFormatPrinterBase::IsSensitive(proto.GetDescriptor(), &field)) {
            PrintSensitiveField(proto, field, json, key);
            return;
        }
        NProtobufJson::TProto2JsonPrinter::PrintField(proto, field, json, key);
    }

private:
    void PrintSensitiveField(const NProtoBuf::Message& proto,
                             const NProtoBuf::FieldDescriptor& field,
                             NProtobufJson::IJsonOutput& json,
                             TStringBuf key) {
        using namespace NProtoBuf;
        const Reflection* reflection = proto.GetReflection();

        TString keyStorage;
        TStringBuf keyBuf = key;
        if (!keyBuf) {
            keyStorage = TString(MakeKey(field));
            keyBuf = keyStorage;
        }

        if (field.is_repeated()) {
            const int fieldSize = reflection->FieldSize(proto, &field);
            const bool isMapObject = field.is_map() && GetConfig().MapAsObject;
            if (fieldSize == 0) {
                switch (GetConfig().MissingRepeatedKeyMode) {
                    case NProtobufJson::TProto2JsonConfig::MissingKeyNull:
                        json.WriteKey(keyBuf).WriteNull();
                        break;
                    case NProtobufJson::TProto2JsonConfig::MissingKeyDefault: {
                        json.WriteKey(keyBuf);
                        if (isMapObject) {
                            json.BeginObject().EndObject();
                        } else {
                            json.BeginList().EndList();
                        }
                        break;
                    }
                    default:
                        break;
                }
                return;
            }
            json.WriteKey(keyBuf);
            if (isMapObject) {
                json.Write(SensitiveJsonPlaceholder());
                return;
            }
            json.BeginList();
            for (int i = 0; i < fieldSize; ++i) {
                json.Write(SensitiveJsonPlaceholder());
            }
            json.EndList();
            return;
        }

        bool shouldPrint = reflection->HasField(proto, &field);
        // Sensitive required fields without defaults are silently skipped rather than throwing.
        // It differs from the base class TProto2JsonPrinter behavior, because we don't want to fail on printing
        shouldPrint = shouldPrint ||
            (GetConfig().MissingSingleKeyMode == NProtobufJson::TProto2JsonConfig::MissingKeyDefault && !field.containing_oneof());

        if (shouldPrint) {
            json.WriteKey(keyBuf).Write(SensitiveJsonPlaceholder());
        } else {
            switch (GetConfig().MissingSingleKeyMode) {
                case NProtobufJson::TProto2JsonConfig::MissingKeyNull:
                    json.WriteKey(keyBuf).WriteNull();
                    break;
                default:
                    break;
            }
        }
    }
};

void SecureProto2JsonImpl(const NProtoBuf::Message& proto,
                          NProtobufJson::IJsonOutput& jsonOut,
                          const NProtobufJson::TProto2JsonConfig& config) {
    TSecurityProto2JsonPrinter printer(config);
    printer.Print(proto, jsonOut, /* closeMap */ true);
}

} // anonymous namespace

void SecureProto2Json(const NProtoBuf::Message& proto,
                      NJson::TJsonWriter& writer,
                      const NProtobufJson::TProto2JsonConfig& config) {
    auto output = NProtobufJson::CreateJsonMapOutput(writer);
    SecureProto2JsonImpl(proto, *output, config);
}

TString SecureProto2JsonString(const NProtoBuf::Message& proto,
                               const NProtobufJson::TProto2JsonConfig& config) {
    TString out;
    auto output = NProtobufJson::CreateJsonMapOutput(out, config);
    SecureProto2JsonImpl(proto, *output, config);
    return out;
}

} // namespace NKikimr
