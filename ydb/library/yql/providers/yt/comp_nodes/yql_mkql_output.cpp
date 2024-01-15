#include "yql_mkql_output.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NYql {

namespace {

using namespace NKikimr::NMiniKQL;

class TYtOutputWrapper : public TDecoratorCodegeneratorNode<TYtOutputWrapper> {
    using TBaseComputation = TDecoratorCodegeneratorNode<TYtOutputWrapper>;
public:
    TYtOutputWrapper(IComputationNode* item, TMkqlWriterImpl& writer)
        : TBaseComputation(item, EValueRepresentation::Embedded), Writer(writer)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        AddRowImpl(value);
        return NUdf::TUnboxedValuePod::Void();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* item, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        if (true /*|| TODO: !Writer.GenAddRow(item, ctx, block)*/) {
            const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtOutputWrapper::AddRowImpl));
            const auto selfArg = ConstantInt::get(Type::getInt64Ty(context), ui64(this));
            const auto arg = WrapArgumentForWindows(item, ctx, block);
            const auto addType = FunctionType::get(Type::getVoidTy(context), {selfArg->getType(), arg->getType()}, false);
            const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "write", block);
            CallInst::Create(addType, addPtr, {selfArg, arg}, "", block);
        }
        if (Node->IsTemporaryValue())
            ValueCleanup(Node->GetRepresentation(), item, ctx, block);
        return GetFalse(context);
    }
#endif
private:
    void AddRowImpl(NUdf::TUnboxedValuePod row) const {
        Writer.AddRow(row);
    }

    TMkqlWriterImpl& Writer;
};

class TYtFlowOutputWrapper : public TStatelessFlowCodegeneratorNode<TYtFlowOutputWrapper> {
using TBaseComputation = TStatelessFlowCodegeneratorNode<TYtFlowOutputWrapper>;
public:
    TYtFlowOutputWrapper(IComputationNode* flow, TMkqlWriterImpl& writer)
        : TBaseComputation(flow, EValueRepresentation::Embedded), Flow(flow), Writer(writer)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (const auto value = Flow->GetValue(ctx); value.IsSpecial())
            return value;
        else
            AddRowImpl(value);
        return NUdf::TUnboxedValuePod::Void();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(item->getType(), 2U, "result", pass);
        result->addIncoming(item, block);

        BranchInst::Create(pass, work, IsSpecial(item, block), block);

        block = work;

        const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtFlowOutputWrapper::AddRowImpl));
        const auto selfArg = ConstantInt::get(Type::getInt64Ty(context), ui64(this));
        const auto arg = WrapArgumentForWindows(item, ctx, block);
        const auto addType = FunctionType::get(Type::getVoidTy(context), {selfArg->getType(), arg->getType()}, false);
        const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "write", block);
        CallInst::Create(addType, addPtr, {selfArg, arg}, "", block);

        ValueCleanup(Flow->GetRepresentation(), item, ctx, block);

        result->addIncoming(ConstantInt::get(item->getType(), 0), block);

        BranchInst::Create(pass, block);

        block = pass;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    void AddRowImpl(NUdf::TUnboxedValuePod row) const {
        Writer.AddRow(row);
    }

    static std::vector<NUdf::TUnboxedValue*> GetPointers(std::vector<NUdf::TUnboxedValue>& array) {
        std::vector<NUdf::TUnboxedValue*> pointers;
        pointers.reserve(array.size());
        std::transform(array.begin(), array.end(), std::back_inserter(pointers), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });
        return pointers;
    }

    IComputationNode *const Flow;
    const std::vector<EValueRepresentation> Representations;

    TMkqlWriterImpl& Writer;

    std::vector<NUdf::TUnboxedValue> Values;
    const std::vector<NUdf::TUnboxedValue*> Fields;
};

class TYtWideOutputWrapper : public TStatelessWideFlowCodegeneratorNode<TYtWideOutputWrapper> {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TYtWideOutputWrapper>;
public:
    TYtWideOutputWrapper(IComputationWideFlowNode* flow, TMkqlWriterImpl& writer, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(flow), Flow(flow), Representations(std::move(representations)), Writer(writer), Values(Representations.size()), Fields(GetPointers(Values))
    {}

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const*) const {
        if (const auto result = Flow->FetchValues(ctx, Fields.data()); EFetchResult::One != result)
            return result;

        AddRowImpl(static_cast<const NUdf::TUnboxedValuePod*>(Values.data()));

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto arrayType = ArrayType::get(valueType, Representations.size());

        const auto values = new AllocaInst(arrayType, 0U, "values", &ctx.Func->getEntryBlock().back());

        const auto result = GetNodeValues(Flow, ctx, block);

        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, result.first, ConstantInt::get(result.first->getType(), 0), "good", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        BranchInst::Create(work, pass, good, block);

        block = work;

        TSmallVec<Value*> fields;
        fields.reserve(Representations.size());
        for (ui32 i = 0U; i < Representations.size(); ++i) {
            const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
            fields.emplace_back(result.second[i](ctx, block));
            new StoreInst(fields.back(), pointer, block);
        }

        const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtWideOutputWrapper::AddRowImpl));
        const auto selfArg = ConstantInt::get(Type::getInt64Ty(context), ui64(this));
        const auto addType = FunctionType::get(Type::getVoidTy(context), {selfArg->getType(), values->getType()}, false);
        const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "write", block);
        CallInst::Create(addType, addPtr, {selfArg, values}, "", block);

        for (ui32 i = 0U; i < Representations.size(); ++i) {
            ValueCleanup(Representations[i], fields[i], ctx, block);
        }

        BranchInst::Create(pass, block);

        block = pass;
        return {result.first, {}};

    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    void AddRowImpl(const NUdf::TUnboxedValuePod* row) const {
        Writer.AddFlatRow(row);
    }

    static std::vector<NUdf::TUnboxedValue*> GetPointers(std::vector<NUdf::TUnboxedValue>& array) {
        std::vector<NUdf::TUnboxedValue*> pointers;
        pointers.reserve(array.size());
        std::transform(array.begin(), array.end(), std::back_inserter(pointers), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });
        return pointers;
    }

    IComputationWideFlowNode *const Flow;
    const std::vector<EValueRepresentation> Representations;

    TMkqlWriterImpl& Writer;

    std::vector<NUdf::TUnboxedValue> Values;
    const std::vector<NUdf::TUnboxedValue*> Fields;
};

}

IComputationNode* WrapYtOutput(TCallable& callable, const TComputationNodeFactoryContext& ctx, TMkqlWriterImpl& writer) {
    YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto item = LocateNode(ctx.NodeLocator, callable, 0);
    if (const auto inputType = callable.GetInput(0).GetStaticType(); inputType->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(item)) {
            std::vector<EValueRepresentation> inputRepresentations;
            auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, inputType));
            inputRepresentations.reserve(wideComponents.size());
            for (ui32 i = 0U; i < wideComponents.size(); ++i)
                inputRepresentations.emplace_back(GetValueRepresentation(wideComponents[i]));
            return new TYtWideOutputWrapper(wide, writer, std::move(inputRepresentations));
        }
        return new TYtFlowOutputWrapper(item, writer);
    }

    return new TYtOutputWrapper(item, writer);
}

} // NYql
