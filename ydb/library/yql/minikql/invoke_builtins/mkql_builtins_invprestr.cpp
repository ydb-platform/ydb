#include "mkql_builtins_impl.h"  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <util/thread/singleton.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TStringDescEncoder : public TPresortEncoder {
    TStringDescEncoder() {
        AddType(NUdf::EDataSlot::String, false, true);
    }
};

NUdf::TUnboxedValuePod Presort(NUdf::TUnboxedValuePod arg) {
    auto encoder = FastTlsSingleton<TStringDescEncoder>();
    encoder->Start();
    encoder->Encode(arg);
    return MakeString(encoder->Finish());
}

NUdf::TUnboxedValuePod Inverse(NUdf::TUnboxedValuePod arg) {
    auto input = arg.AsStringRef();
    auto ret = MakeStringNotFilled(input.Size());
    auto readPtr = input.Data();
    auto writePtr = ret.AsStringRef().Data();
    for (ui32 i = 0; i < input.Size(); ++i) {
        writePtr[i] = readPtr[i] ^ 0xff;
    }

    return ret;
}

template <typename TInput, typename TOutput>
struct TInversePresortString {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        return Presort(arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return CallUnaryUnboxedValueFunction(&Presort, Type::getInt128Ty(ctx.Codegen.GetContext()), arg, ctx.Codegen, block);
    }
#endif
};

template <typename TInput, typename TOutput>
struct TInverseString {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg)
    {
        return Inverse(arg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return CallUnaryUnboxedValueFunction(&Inverse, Type::getInt128Ty(ctx.Codegen.GetContext()), arg, ctx.Codegen, block);
    }
#endif
};

}

void RegisterInversePresortString(IBuiltinFunctionRegistry& registry) {
    const auto name = "InversePresortString";
    RegisterFunction<NUdf::TDataType<char*>, NUdf::TDataType<char*>, TInversePresortString, TUnaryArgs>(registry, name);
    RegisterFunction<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<char*>, TInversePresortString, TUnaryArgs>(registry, name);
}

void RegisterInverseString(IBuiltinFunctionRegistry& registry) {
    const auto name = "InverseString";
    RegisterFunction<NUdf::TDataType<char*>, NUdf::TDataType<char*>, TInverseString, TUnaryArgs>(registry, name);
}

} // namespace NMiniKQL
} // namespace NKikimr
