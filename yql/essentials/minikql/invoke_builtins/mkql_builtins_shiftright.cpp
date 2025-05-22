#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TShiftRight : public TShiftArithmeticBinary<TInput, TOutput, TShiftRight<TInput, TOutput>> {
    static TOutput Do(TInput arg, ui8 bits)
    {
        return (bits < sizeof(arg) * CHAR_BIT) ? (arg >> bits) : 0;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, Value* bits, const TCodegenContext&, BasicBlock*& block)
    {
        const auto zero = ConstantInt::get(arg->getType(), 0);
        const auto maxb = ConstantInt::get(bits->getType(), sizeof(TInput) * CHAR_BIT);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, bits, maxb, "check", block);
        const auto zext = arg->getType() == bits->getType() ? bits : new ZExtInst(bits, arg->getType(), "zext", block);
        const auto lshr = BinaryOperator::CreateLShr(arg, zext, "lshr", block);
        const auto res = SelectInst::Create(check, lshr, zero, "res", block);
        return res;
    }
#endif
};

}

void RegisterShiftRight(IBuiltinFunctionRegistry& registry) {
    RegisterUnsignedShiftFunctionOpt<TShiftRight, TBinaryShiftArgsOpt>(registry, "ShiftRight");
}

} // namespace NMiniKQL
} // namespace NKikimr
