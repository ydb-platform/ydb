#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TRotLeft : public TShiftArithmeticBinary<TInput, TOutput, TRotLeft<TInput, TOutput>> {
    static TOutput Do(TInput arg, ui8 bits)
    {
        bits %= (sizeof(arg) * CHAR_BIT);
        return bits ? (arg << bits) | (arg >> (sizeof(arg) * CHAR_BIT - bits)) : arg;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, Value* bits, const TCodegenContext&, BasicBlock*& block)
    {
        const auto maxb = ConstantInt::get(arg->getType(), sizeof(TInput) * CHAR_BIT);
        const auto zext = arg->getType() == bits->getType() ? bits : new ZExtInst(bits, arg->getType(), "zext", block);
        const auto rem = BinaryOperator::CreateURem(zext, maxb, "rem", block);
        const auto shl = BinaryOperator::CreateShl(arg, rem, "shl", block);
        const auto sub = BinaryOperator::CreateSub(maxb, rem, "sub", block);
        const auto lshr = BinaryOperator::CreateLShr(arg, sub, "lshr", block);
        const auto res = BinaryOperator::CreateOr(shl, lshr, "res", block);
        return res;
    }
#endif
};

}

void RegisterRotLeft(IBuiltinFunctionRegistry& registry) {
    RegisterUnsignedShiftFunctionOpt<TRotLeft, TBinaryShiftArgsOpt>(registry, "RotLeft");
}

} // namespace NMiniKQL
} // namespace NKikimr
