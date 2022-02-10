#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>

namespace NYql::NDq {

class TMkqlValueRef {
public:
    TMkqlValueRef(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value)
        : Type(&type)
        , Value(&value) {}

    explicit TMkqlValueRef(const NKikimrMiniKQL::TParams& params)
        : TMkqlValueRef(params.GetType(), params.GetValue()) {}

    explicit TMkqlValueRef(const NKikimrMiniKQL::TResult& result)
        : TMkqlValueRef(result.GetType(), result.GetValue()) {}

    const NKikimrMiniKQL::TType& GetType() const { return *Type; }

    const NKikimrMiniKQL::TValue& GetValue() const { return *Value; }

private:
    const NKikimrMiniKQL::TType *Type;
    const NKikimrMiniKQL::TValue *Value;
};

}
