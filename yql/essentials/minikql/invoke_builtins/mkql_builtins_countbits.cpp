#include "mkql_builtins_impl.h"  // Y_IGNORE  // Y_IGNORE

#include <bit>
#include <type_traits>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TCountBits : public TSimpleArithmeticUnary<TInput, TOutput, TCountBits<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        if constexpr (std::is_signed_v<TInput>) {
            return std::popcount(static_cast<std::make_unsigned_t<TInput>>(val));
        } else {
            return std::popcount(val);
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        auto& module = ctx.Codegen.GetModule();
        const auto fnType = FunctionType::get(arg->getType(), {arg->getType()}, false);
        const auto& name = GetFuncNameForType<TInput>("llvm.ctpop");
        const auto func = module.getOrInsertFunction(name, fnType).getCallee();
        const auto result = CallInst::Create(fnType, func, {arg}, "popcount", block);
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
