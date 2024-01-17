#pragma once

#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NYamlConfig {

inline NProtobufJson::TProto2JsonConfig GetProto2JsonConfig() {
    return NProtobufJson::TProto2JsonConfig()
        .SetFormatOutput(false)
        .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName)
        .SetFieldNameMode(NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense)
        .SetStringifyNumbers(NProtobufJson::TProto2JsonConfig::StringifyLongNumbersForDouble);
}

} // NYamlConfig
