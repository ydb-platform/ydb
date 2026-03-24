#pragma once

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <library/cpp/enumbitset/enumbitset.h>

namespace NYql::NCommon::NJsonCodec {

using namespace NKikimr;

enum EValueConvertPolicy {
    CONVERT_POLICY_BEGIN,
    NUMBER_AS_STRING = CONVERT_POLICY_BEGIN,
    UNSAFE_NUMBER_AS_STRING,
    BOOL_AS_STRING,
    DISALLOW_NaN,
    CONVERT_POLICY_END
};

using TValueConvertPolicy = TEnumBitSet<EValueConvertPolicy, EValueConvertPolicy::CONVERT_POLICY_BEGIN, EValueConvertPolicy::CONVERT_POLICY_END>;

class DefaultPolicy {
public:
    static DefaultPolicy& GetInstance() {
        static DefaultPolicy Instance;
        return Instance;
    }

private:
    DefaultPolicy() {
    }

public:
    DefaultPolicy(DefaultPolicy&) = delete;
    void operator=(const DefaultPolicy&) = delete;

    TValueConvertPolicy CloudFunction() const {
        return CloudFunctionPolicy_;
    }

    TValueConvertPolicy Export() const {
        return ExportPolicy_;
    }

private:
    TValueConvertPolicy CloudFunctionPolicy_ = TValueConvertPolicy{NUMBER_AS_STRING, BOOL_AS_STRING};
    TValueConvertPolicy ExportPolicy_ = TValueConvertPolicy{DISALLOW_NaN};
};

NJson::TJsonWriterConfig MakeJsonConfig();

void WriteValueToJson(NJson::TJsonWriter& writer, const NUdf::TUnboxedValuePod& value,
                      NMiniKQL::TType* type, TValueConvertPolicy convertPolicy = {});

NUdf::TUnboxedValue ReadJsonValue(NJson::TJsonValue& json, NMiniKQL::TType* type, const NMiniKQL::THolderFactory& holderFactory);
} // namespace NYql::NCommon::NJsonCodec
