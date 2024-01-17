#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TRotRight : public TShiftArithmeticBinary<TInput, TOutput, TRotRight<TInput, TOutput>> {
    static TOutput Do(TInput arg, ui8 bits)
    {
        bits %= (sizeof(arg) * CHAR_BIT);
        return bits ? ((arg >> bits) | (arg << (sizeof(arg) * CHAR_BIT - bits))) : arg;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, Value* bits, const TCodegenContext&, BasicBlock*& block)
    {
        const auto maxb = ConstantInt::get(arg->getType(), sizeof(TInput) * CHAR_BIT);
        const auto zext = arg->getType() == bits->getType() ? bits : new ZExtInst(bits, arg->getType(), "zext", block);
        const auto rem = BinaryOperator::CreateURem(zext, maxb, "rem", block);
        const auto lshr = BinaryOperator::CreateLShr(arg, rem, "lshr", block);
        const auto sub = BinaryOperator::CreateSub(maxb, rem, "sub", block);
        const auto shl = BinaryOperator::CreateShl(arg, sub, "shl", block);
        const auto res = BinaryOperator::CreateOr(shl, lshr, "res", block);
        return res;
    }
#endif
};

}

void RegisterRotRight(IBuiltinFunctionRegistry& registry) {
    RegisterUnsignedShiftFunctionOpt<TRotRight, TBinaryShiftArgsOpt>(registry, "RotRight");
}

} // namespace NMiniKQL
} // namespace NKikimr
