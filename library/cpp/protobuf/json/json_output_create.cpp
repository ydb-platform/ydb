#include "json_output_create.h"

#include "config.h"
#include "json_writer_output.h"
#include "json_value_output.h"

namespace NProtobufJson {
    TJsonMapOutputPtr CreateJsonMapOutput(IOutputStream& out, const NJson::TJsonWriterConfig& config) {
        return MakeHolder<TJsonWriterOutput>(&out, config);
    }

    TJsonMapOutputPtr CreateJsonMapOutput(NJson::TJsonWriter& writer) {
        return MakeHolder<TBaseJsonWriterOutput>(writer);
    }

    TJsonMapOutputPtr CreateJsonMapOutput(TString& str, const TProto2JsonConfig& config) {
        return MakeHolder<TJsonStringWriterOutput>(&str, config);
    }

    TJsonMapOutputPtr CreateJsonMapOutput(TStringStream& out, const TProto2JsonConfig& config) {
        return MakeHolder<TJsonWriterOutput>(&out, config);
    }

    TJsonMapOutputPtr CreateJsonMapOutput(IOutputStream& out, const TProto2JsonConfig& config) {
        return MakeHolder<TJsonWriterOutput>(&out, config);
    }

    TJsonMapOutputPtr CreateJsonMapOutput(NJson::TJsonValue& json) {
        return MakeHolder<TJsonValueOutput>(json);
    }

}
