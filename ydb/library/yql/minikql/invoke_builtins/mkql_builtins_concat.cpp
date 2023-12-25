#include "mkql_builtins_impl.h"  // Y_IGNORE  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TLeft, typename TRight, typename TOutput>
struct TConcat {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod left, NUdf::TUnboxedValuePod right)
    {
        return ConcatStrings(std::move(left), std::move(right));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        return CallBinaryUnboxedValueFunction(&ConcatStrings, Type::getInt128Ty(ctx.Codegen.GetContext()), left, right, ctx.Codegen, block);
    }
#endif
};

template<typename TType>
using TAggrConcat = TConcat<TType, TType, TType>;

}

void RegisterConcat(IBuiltinFunctionRegistry& registry) {
    const auto name = "Concat";
    RegisterFunctionBinOpt<NUdf::TDataType<char*>, NUdf::TDataType<char*>, NUdf::TDataType<char*>, TConcat, TBinaryArgsOpt>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<char*>, NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<char*>, TConcat, TBinaryArgsOpt>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<char*>, NUdf::TDataType<char*>, TConcat, TBinaryArgsOpt>(registry, name);
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<NUdf::TUtf8>, TConcat, TBinaryArgsOpt>(registry, name);

    const auto aggrName = "AggrConcat";
    RegisterAggregateFunction<NUdf::TDataType<char*>, TAggrConcat, TBinaryArgsSameOpt>(registry, aggrName);
    RegisterAggregateFunction<NUdf::TDataType<NUdf::TUtf8>, TAggrConcat, TBinaryArgsSameOpt>(registry, aggrName);
}

} // namespace NMiniKQL
} // namespace NKikimr
