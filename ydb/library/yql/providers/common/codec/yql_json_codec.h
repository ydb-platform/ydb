#pragma once

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

using namespace NKikimr;

namespace NYql {
namespace NCommon {

enum class EValueConvertPolicy : ui8 {
    WriteNumberString = 1,
    WriteUnsafeNumberString = 2,
    //WriteNumberAsString = 4,
    //WriteNumberAsString = 8,
};

NJson::TJsonWriterConfig MakeJsonConfig();

void WriteValueToJson(NJson::TJsonWriter& writer, const NUdf::TUnboxedValuePod& value,
                      NMiniKQL::TType* type, std::set<EValueConvertPolicy> convertPolicy = {});

NUdf::TUnboxedValue ReadJsonValue(NJson::TJsonValue& json, NMiniKQL::TType* type, const NMiniKQL::THolderFactory& holderFactory);

NUdf::TUnboxedValue ReadJsonValue(IInputStream* in, NMiniKQL::TType* type, const NMiniKQL::THolderFactory& holderFactory);

}
}
