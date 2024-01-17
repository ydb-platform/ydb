#include "mkql_builtins_impl.h"  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TShiftLeft : public TShiftArithmeticBinary<TInput, TOutput, TShiftLeft<TInput, TOutput>> {
    static TOutput Do(TInput arg, ui8 bits)
    {
        return (bits < sizeof(arg) * CHAR_BIT) ? (arg << bits) : 0;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, Value* bits, const TCodegenContext&, BasicBlock*& block)
    {
        const auto zero = ConstantInt::get(arg->getType(), 0);
        const auto maxb = ConstantInt::get(bits->getType(), sizeof(TInput) * CHAR_BIT);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, bits, maxb, "check", block);
        const auto zext = arg->getType() == bits->getType() ? bits : new ZExtInst(bits, arg->getType(), "zext", block);
        const auto shl = BinaryOperator::CreateShl(arg, zext, "shl", block);
        const auto res = SelectInst::Create(check, shl, zero, "res", block);
        return res;
    }
#endif
};

}

void RegisterShiftLeft(IBuiltinFunctionRegistry& registry) {
    RegisterUnsignedShiftFunctionOpt<TShiftLeft, TBinaryShiftArgsOpt>(registry, "ShiftLeft");
}

} // namespace NMiniKQL
} // namespace NKikimr
