#include "stlog.h"
#include <library/cpp/json/json_reader.h>
#include <google/protobuf/util/json_util.h>

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

} // NKikimr::NStLog
