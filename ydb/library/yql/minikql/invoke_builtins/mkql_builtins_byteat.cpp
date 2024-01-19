#include "mkql_builtins_impl.h"  // Y_IGNORE 

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TInput, typename TOutput, bool IsOptional>
struct TByteAtArgs {
    static const TFunctionParamMetadata Value[4];
};

template <typename TInput, typename TOutput, bool IsOptional>
const TFunctionParamMetadata TByteAtArgs<TInput, TOutput, IsOptional>::Value[4] = {
    { TOutput::Id, TFunctionParamMetadata::FlagIsNullable },
    { TInput::Id, IsOptional ? TFunctionParamMetadata::FlagIsNullable : 0 },
    { NUdf::TDataType<ui32>::Id, 0 },
    { 0, 0 }
};


template <typename TInput, typename TOutput>
struct TByteAt {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto& buffer = left.AsStringRef();
        const auto index = right.Get<ui32>();
        if (index >= buffer.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(ui8(buffer.Data()[index]));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto type = Type::getInt8Ty(context);
        const auto embType = FixedVectorType::get(type, 16);
        const auto cast = CastInst::Create(Instruction::BitCast, left, embType, "cast", block);
        const auto mark = ExtractElementInst::Create(cast, ConstantInt::get(type, 15), "mark", block);
        const auto index = GetterFor<ui32>(right, context, block);

        const auto bsize = ExtractElementInst::Create(cast, ConstantInt::get(type, 14), "bsize", block);
        const auto esize = CastInst::Create(Instruction::ZExt, bsize, index->getType(), "esize", block);

        const auto sizeType = Type::getInt32Ty(context);
        const auto strType = FixedVectorType::get(sizeType, 4);
        const auto four = CastInst::Create(Instruction::BitCast, left, strType, "four", block);
        const auto ssize = ExtractElementInst::Create(four, ConstantInt::get(type, 2), "ssize", block);

        const auto cemb = CastInst::Create(Instruction::Trunc, mark, Type::getInt1Ty(context), "cemb", block);
        const auto size = SelectInst::Create(cemb, esize, ssize, "size", block);
        const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, index, size, "ok", block);

        const auto sel = BasicBlock::Create(context, "sel", ctx.Func);
        const auto emb = BasicBlock::Create(context, "emb", ctx.Func);
        const auto str = BasicBlock::Create(context, "str", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto zero = ConstantInt::get(left->getType(), 0);
        const auto result = PHINode::Create(left->getType(), 4, "result", done);
        result->addIncoming(zero, block);

        BranchInst::Create(sel, done, ok, block);
        block = sel;

        result->addIncoming(zero, block);
        const auto choise = SwitchInst::Create(mark, done, 2, block);
        choise->addCase(ConstantInt::get(type, 1), emb);
        choise->addCase(ConstantInt::get(type, 2), str);

        {
            block = emb;

            const auto byte = ExtractElementInst::Create(cast, index, "byte", block);
            const auto full = SetterFor<ui8>(byte, context, block);
            result->addIncoming(full, block);
            BranchInst::Create(done, block);
        }

        {
            block = str;

            const auto foffs = ExtractElementInst::Create(four, ConstantInt::get(type, 3), "foffs", block);
            const auto offs = BinaryOperator::CreateAnd(foffs, ConstantInt::get(foffs->getType(), 0xFFFFFF), "offs", block);
            const auto skip = BinaryOperator::CreateAdd(offs, ConstantInt::get(offs->getType(), 16), "skip", block);
            const auto pos = BinaryOperator::CreateAdd(index, skip, "pos", block);

            const auto half = CastInst::Create(Instruction::Trunc, left, Type::getInt64Ty(context), "half", block);
            const auto ptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type) , "ptr", block);

            const auto bytePtr = GetElementPtrInst::CreateInBounds(type, ptr, {pos}, "bptr", block);
            const auto got = new LoadInst(type, bytePtr, "got", block);
            const auto make = SetterFor<ui8>(got, context, block);
            result->addIncoming(make, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
};

}

void RegisterByteAt(IBuiltinFunctionRegistry& registry) {
    const auto name = "ByteAt";
    RegisterFunctionImpl<TByteAt<NUdf::TDataType<char*>, NUdf::TDataType<ui8>>,
        TByteAtArgs<NUdf::TDataType<char*>, NUdf::TDataType<ui8>, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TByteAt<NUdf::TDataType<char*>, NUdf::TDataType<ui8>>,
        TByteAtArgs<NUdf::TDataType<char*>, NUdf::TDataType<ui8>, true>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TByteAt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<ui8>>,
        TByteAtArgs<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<ui8>, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TByteAt<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<ui8>>,
        TByteAtArgs<NUdf::TDataType<NUdf::TUtf8>, NUdf::TDataType<ui8>, true>, TBinaryWrap<true, false>>(registry, name);
}

} // namespace NMiniKQL
} // namespace NKikimr
