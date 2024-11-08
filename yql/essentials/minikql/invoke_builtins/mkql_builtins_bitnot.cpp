#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TBitNot : public TSimpleArithmeticUnary<TInput, TOutput, TBitNot<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        return ~val;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        return BinaryOperator::CreateNot(arg, "not", block);
    }
#endif
};

}

void RegisterBitNot(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryUnsignedFunctionOpt<TBitNot, TUnaryArgsOpt>(registry, "BitNot");
}

} // namespace NMiniKQL
} // namespace NKikimr
