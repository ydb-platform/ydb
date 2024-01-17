#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TBitOr : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TBitOr<TLeft, TRight, TOutput>> {
    static TOutput Do(TOutput left, TOutput right)
    {
        return left | right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return BinaryOperator::CreateOr(left, right, "or", block);
    }
#endif
};

}

void RegisterBitOr(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryUnsignedFunctionOpt<TBitOr, TBinaryArgsOpt>(registry, "BitOr");
}

} // namespace NMiniKQL
} // namespace NKikimr
