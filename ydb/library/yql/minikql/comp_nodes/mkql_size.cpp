#include "mkql_size.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

extern "C" void DeleteString(void* strData);

template<size_t Size>
class TSizePrimitiveTypeWrapper : public TDecoratorCodegeneratorNode<TSizePrimitiveTypeWrapper<Size>> {
    typedef TDecoratorCodegeneratorNode<TSizePrimitiveTypeWrapper<Size>> TBaseComputation;
public:
    TSizePrimitiveTypeWrapper(IComputationNode* data)
        : TBaseComputation(data)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return value ? NUdf::TUnboxedValuePod(ui32(Size)) : NUdf::TUnboxedValuePod();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const {
        Y_UNUSED(ctx);
        const uint64_t init[] = {Size, 0x100000000000000ULL};
        const auto size = ConstantInt::get(value->getType(), APInt(128, 2, init));
        return SelectInst::Create(IsEmpty(value, block), value, size, "size", block);
    }
#endif
};

template<bool IsOptional>
class TSizeWrapper : public TMutableCodegeneratorNode<TSizeWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TSizeWrapper<IsOptional>> TBaseComputation;
public:
    TSizeWrapper(TComputationMutables& mutables, IComputationNode* data)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Data(data)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& data = Data->GetValue(ctx);
        if (IsOptional && !data) {
            return NUdf::TUnboxedValuePod();
        }

        const ui32 size = data.AsStringRef().Size();
        return NUdf::TUnboxedValuePod(size);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto data = GetNodeValue(this->Data, ctx, block);

        const auto type = Type::getInt8Ty(context);
        const auto embType = FixedVectorType::get(type, 16);
        const auto cast = CastInst::Create(Instruction::BitCast, data, embType, "cast", block);

        const auto mark = ExtractElementInst::Create(cast, {ConstantInt::get(type, 15)}, "mark", block);
        const auto bsize = ExtractElementInst::Create(cast, {ConstantInt::get(type, 14)}, "bsize", block);

        const auto emb = BasicBlock::Create(context, "emb", ctx.Func);
        const auto str = BasicBlock::Create(context, "str", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(data->getType(), 4, "result", done);
        result->addIncoming(ConstantInt::get(data->getType(), 0), block);

        const auto choise = SwitchInst::Create(mark, done, 2, block);
        choise->addCase(ConstantInt::get(type, 1), emb);
        choise->addCase(ConstantInt::get(type, 2), str);

        {
            block = emb;

            const auto full = SetterFor<ui32>(bsize, context, block);
            result->addIncoming(full, block);
            BranchInst::Create(done, block);
        }

        {
            block = str;

            const auto type32 = Type::getInt32Ty(context);
            const auto strType = FixedVectorType::get(type32, 4);
            const auto four = CastInst::Create(Instruction::BitCast, data, strType, "four", block);
            const auto ssize = ExtractElementInst::Create(four, {ConstantInt::get(type, 2)}, "ssize", block);
            const auto full = SetterFor<ui32>(ssize, context, block);

            const auto half = CastInst::Create(Instruction::Trunc, data, Type::getInt64Ty(context), "half", block);
            const auto strptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(strType), "str_ptr", block);
            const auto refptr = GetElementPtrInst::CreateInBounds(strType, strptr, {ConstantInt::get(type32, 0), ConstantInt::get(type32, 1)}, "refptr", block);
            const auto refs = new LoadInst(type32, refptr, "refs", block);
            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, refs, ConstantInt::get(refs->getType(), 0), "test", block);

            const auto free = BasicBlock::Create(context, "free", ctx.Func);

            result->addIncoming(full, block);
            BranchInst::Create(done, free, test, block);

            block = free;

            const auto fnType = FunctionType::get(Type::getVoidTy(context), {strptr->getType()}, false);
            const auto name = "DeleteString";
            ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DeleteString));
            const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
            CallInst::Create(func, {strptr}, "", block);
            result->addIncoming(full, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    IComputationNode* const Data;
};

}

IComputationNode* WrapSize(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);

    const auto data = LocateNode(ctx.NodeLocator, callable, 0);

    switch(dataType->GetSchemeType()) {
#define MAKE_PRIMITIVE_TYPE_SIZE(type, layout) \
        case NUdf::TDataType<type>::Id: \
            if (isOptional) \
                return new TSizePrimitiveTypeWrapper<sizeof(layout)>(data); \
            else \
                return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(ui32(sizeof(layout))));
        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_SIZE)
#undef MAKE_PRIMITIVE_TYPE_SIZE
    }

    if (isOptional)
        return new TSizeWrapper<true>(ctx.Mutables, data);
    else
        return new TSizeWrapper<false>(ctx.Mutables, data);
}

}
}
