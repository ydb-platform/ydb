#include "kqp_compute.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetKqpBaseComputeFactory(const TKqpComputeContextBase* computeCtx) {
    return NYql::NDq::GetDqBaseComputeFactory(computeCtx);
}

namespace {

class TKqpEnsureWrapper : public TMutableCodegeneratorNode<TKqpEnsureWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TKqpEnsureWrapper>;
public:
    TKqpEnsureWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* predicate,
        IComputationNode* issueCode, IComputationNode* message)
        : TBaseComputation(mutables, value->GetRepresentation())
        , Arg(value)
        , Predicate(predicate)
        , IssueCode(issueCode)
        , Message(message)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& predicate = Predicate->GetValue(ctx);
        if (predicate && predicate.Get<bool>()) {
            return Arg->GetValue(ctx).Release();
        }

        Throw(this, &ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();

        const auto predicate = GetNodeValue(Predicate, ctx, block);
        const auto pass = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(good, kill, pass, block);

        block = kill;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TKqpEnsureWrapper::Throw));
        const auto doFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);
        const auto doFuncType = FunctionType::get(Type::getVoidTy(context), { Type::getInt64Ty(context), ctx.Ctx->getType() }, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
        CallInst::Create(doFuncType, doFuncPtr, { doFuncArg, ctx.Ctx }, "", block)->setTailCall();
        new UnreachableInst(context, block);

        block = good;
        return GetNodeValue(Arg, ctx, block);;
    }
#endif

private:
    [[noreturn]]
    static void Throw(TKqpEnsureWrapper const* thisPtr, TComputationContext* ctxPtr) {
        auto issueCode = thisPtr->IssueCode->GetValue(*ctxPtr);
        auto message = thisPtr->Message->GetValue(*ctxPtr);

        throw TKqpEnsureFail(issueCode.Get<ui32>(), TString(TStringBuf(message.AsStringRef())));
    }

    void RegisterDependencies() const final {
        DependsOn(Arg);
        DependsOn(Predicate);
    }

    IComputationNode* const Arg;
    IComputationNode* const Predicate;
    IComputationNode* const IssueCode;
    IComputationNode* const Message;
};

} // namespace

IComputationNode* WrapKqpEnsure(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    bool isOptional;
    auto unpackedType = UnpackOptionalData(callable.GetInput(1), isOptional);
    MKQL_ENSURE(unpackedType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    auto value = LocateNode(ctx.NodeLocator, callable, 0);
    auto predicate = LocateNode(ctx.NodeLocator, callable, 1);
    auto issueCode = LocateNode(ctx.NodeLocator, callable, 2);
    auto message = LocateNode(ctx.NodeLocator, callable, 3);

    return new TKqpEnsureWrapper(ctx.Mutables, value, predicate, issueCode, message);
}

} // namespace NMiniKQL
} // namespace NKikimr
