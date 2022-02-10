#include "proto2json.h"

#include "json_output_create.h"
#include "proto2json_printer.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

namespace NProtobufJson {
    void Proto2Json(const NProtoBuf::Message& proto, IJsonOutput& jsonOutput,
                    const TProto2JsonConfig& config, bool closeMap) {
        TProto2JsonPrinter printer(config);
        printer.Print(proto, jsonOutput, closeMap);
    }

    void Proto2Json(const NProtoBuf::Message& proto, NJson::TJsonValue& json,
                    const TProto2JsonConfig& config) {
        Proto2Json(proto, *CreateJsonMapOutput(json), config);
    }

    void Proto2Json(const NProtoBuf::Message& proto, NJson::TJsonWriter& writer,
                    const TProto2JsonConfig& config) {
        Proto2Json(proto, *CreateJsonMapOutput(writer), config);
        writer.Flush();
    }

    void Proto2Json(const NProtoBuf::Message& proto, IOutputStream& out,
                    const TProto2JsonConfig& config) {
        Proto2Json(proto, *CreateJsonMapOutput(out, config), config);
    }

    void Proto2Json(const NProtoBuf::Message& proto, TStringStream& out,
                    const TProto2JsonConfig& config) {
        Proto2Json(proto, *CreateJsonMapOutput(out, config), config);
    }

    void Proto2Json(const NProtoBuf::Message& proto, TString& str,
                    const TProto2JsonConfig& config) {
        Proto2Json(proto, *CreateJsonMapOutput(str, config), config);
    }

    TString Proto2Json(const ::NProtoBuf::Message& proto,
                       const TProto2JsonConfig& config) {
        TString res;
        Proto2Json(proto, res, config);
        return res;
    }

}
