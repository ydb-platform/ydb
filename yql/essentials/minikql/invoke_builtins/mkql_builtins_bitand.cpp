#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TBitAnd : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TBitAnd<TLeft, TRight, TOutput>> {
    static TOutput Do(TOutput left, TOutput right)
    {
        return left & right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return BinaryOperator::CreateAnd(left, right, "and", block);
    }
#endif
};

}

void RegisterBitAnd(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryUnsignedFunctionOpt<TBitAnd, TBinaryArgsOpt>(registry, "BitAnd");
}

} // namespace NMiniKQL
} // namespace NKikimr
