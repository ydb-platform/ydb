#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TBitXor : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TBitXor<TLeft, TRight, TOutput>> {
    static TOutput Do(TOutput left, TOutput right)
    {
        return left ^ right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return BinaryOperator::CreateXor(left, right, "xor", block);
    }
#endif
};

}

void RegisterBitXor(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryUnsignedFunctionOpt<TBitXor, TBinaryArgsOpt>(registry, "BitXor");
}

} // namespace NMiniKQL
} // namespace NKikimr
