#include "stlog.h"
#include <library/cpp/json/json_reader.h>
#include <google/protobuf/util/json_util.h>
#include <util/stream/output.h>
#include <google/protobuf/text_format.h>
#include <cstring>

namespace NKikimr::NStLog {

    bool OutputLogJson = false;

    void ProtobufToJson(const NProtoBuf::Message& m, NJson::TJsonWriter& json) {
        TString s;
        google::protobuf::util::MessageToJsonString(m, &s);
        if (s) {
            json.UnsafeWrite(s);
        } else {
            json.Write("protobuf deserialization error");
        }
    }

    // Non-template helper functions to reduce binary bloat
    // These handle complex cases that would otherwise bloat each template instantiation
    
    void OutputProtobufMessage(IOutputStream& s, const google::protobuf::Message& value) {
        google::protobuf::TextFormat::Printer p;
        p.SetSingleLineMode(true);
        TString str;
        if (p.PrintToString(value, &str)) {
            s << "{" << str << "}";
        } else {
            s << "<error>";
        }
    }

    void OutputProtobufEnum(IOutputStream& s, int enumValue, const google::protobuf::EnumDescriptor* descriptor) {
        if (descriptor) {
            if (const auto* val = descriptor->FindValueByNumber(enumValue)) {
                s << val->name();
            } else {
                s << enumValue;
            }
        } else {
            s << enumValue;
        }
    }

    void OutputBool(IOutputStream& s, bool value) {
        s << (value ? "true" : "false");
    }

    void OutputNull(IOutputStream& s) {
        s << "<null>";
    }

    // Helper function to extract filename from full path
    const char* GetFileName(const char* file) {
        const char *p = strrchr(file, '/');
        return p ? p + 1 : file;
    }

    // Helper function to write message header to stream
    void WriteMessageHeader(IOutputStream& s, const char* marker, const char* file, int line) {
        s << "{" << marker << "@" << GetFileName(file) << ":" << line << "} ";
    }

    // Helper function to output protobuf enum to JSON
    void OutputProtobufEnumToJson(NJson::TJsonWriter& json, int enumValue, const google::protobuf::EnumDescriptor* descriptor) {
        if (descriptor) {
            if (const auto *val = descriptor->FindValueByNumber(enumValue)) {
                json.Write(val->name());
            } else {
                json.Write(static_cast<int>(enumValue));
            }
        } else {
            json.Write(static_cast<int>(enumValue));
        }
    }

    // Helper function to write JSON message header (marker, file, line)
    void WriteJsonMessageHeader(NJson::TJsonWriter& json, const char* marker, const char* file, int line) {
        if (*file) {
            json.WriteKey("marker");
            json.Write(marker);
            json.WriteKey("file");
            json.Write(GetFileName(file));
            json.WriteKey("line");
            json.Write(line);
        }
    }

} // NKikimr::NStLog
