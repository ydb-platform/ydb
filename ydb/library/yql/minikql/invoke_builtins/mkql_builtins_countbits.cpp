#include "mkql_builtins_impl.h"  // Y_IGNORE  // Y_IGNORE

#include <library/cpp/pop_count/popcount.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TCountBitsBase {
#ifndef MKQL_DISABLE_CODEGEN
    static Value* GenImpl(Value* arg, const TCodegenContext& ctx, BasicBlock*& block, const std::string& funcName) {
        auto& module = ctx.Codegen.GetModule();
        const auto fnType = FunctionType::get(arg->getType(), {arg->getType()}, false);
        const auto& name = funcName;
        const auto func = module.getOrInsertFunction(name, fnType).getCallee();
        const auto result = CallInst::Create(fnType, func, {arg}, "popcount", block);
        return result;
    }
#endif
};

template<typename TInput, typename TOutput>
struct TCountBits : public TSimpleArithmeticUnary<TInput, TOutput, TCountBits<TInput, TOutput>>, TCountBitsBase {
    static TOutput Do(TInput val)
    {
        return PopCount(val);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto result = GenImpl(arg, ctx, block, GetFuncNameForType<TInput>("llvm.ctpop"));
        return StaticCast<TInput, TOutput>(result, context, block);
    }
#endif
};

}

void RegisterCountBits(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryIntegralFunctionOpt<TCountBits, TUnaryArgsOpt>(registry, "CountBits");
}

} // namespace NMiniKQL
} // namespace NKikimr
