#pragma once

#include "config.h"
#include "json_output.h"

namespace NJson {
    class TJsonValue;
    class TJsonWriter;
    struct TJsonWriterConfig;
}

class IOutputStream;
class TStringStream;

namespace NProtobufJson {
    TJsonMapOutputPtr CreateJsonMapOutput(IOutputStream& out, const NJson::TJsonWriterConfig& config);
    TJsonMapOutputPtr CreateJsonMapOutput(NJson::TJsonWriter& writer);
    TJsonMapOutputPtr CreateJsonMapOutput(IOutputStream& out, const TProto2JsonConfig& config = TProto2JsonConfig());
    TJsonMapOutputPtr CreateJsonMapOutput(TString& str, const TProto2JsonConfig& config = TProto2JsonConfig());
    TJsonMapOutputPtr CreateJsonMapOutput(NJson::TJsonValue& json);

}
