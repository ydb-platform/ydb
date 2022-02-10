#include "json_writer_output.h"

namespace NProtobufJson {
    NJson::TJsonWriterConfig TJsonWriterOutput::CreateJsonWriterConfig(const TProto2JsonConfig& config) { 
        NJson::TJsonWriterConfig jsonConfig; 
        jsonConfig.FormatOutput = config.FormatOutput; 
        jsonConfig.SortKeys = false; 
        jsonConfig.ValidateUtf8 = false; 
        jsonConfig.DontEscapeStrings = false; 
        jsonConfig.WriteNanAsString = config.WriteNanAsString;

        for (size_t i = 0; i < config.StringTransforms.size(); ++i) { 
            Y_ASSERT(config.StringTransforms[i]); 
            if (config.StringTransforms[i]->GetType() == IStringTransform::EscapeTransform) { 
                jsonConfig.DontEscapeStrings = true; 
                break; 
            } 
        }
        return jsonConfig; 
    }
 
}
