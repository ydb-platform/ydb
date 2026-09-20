#include "mkql_ensure.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/public/udf/udf_terminator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

class TEnsureWrapper: public TMutableCodegeneratorNode<TEnsureWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TEnsureWrapper>;

public:
    TEnsureWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* predicate,
                   IComputationNode* message, const NUdf::TSourcePosition& pos)
        : TBaseComputation(mutables, value->GetRepresentation())
        , Arg_(value)
        , Predicate_(predicate)
        , Message_(message)
        , Pos_(pos)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& predicate = Predicate_->GetValue(ctx);
        if (predicate && predicate.Get<bool>()) {
            return Arg_->GetValue(ctx).Release();
        }

        Throw(this, &ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto predicate = GetNodeValue(Predicate_, ctx, block);
        const auto pass = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(good, kill, pass, block);

        block = kill;
        const auto doFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);
        EmitFunctionCall<&TEnsureWrapper::Throw>(Type::getVoidTy(context), {doFuncArg, ctx.Ctx}, ctx, block);
        new UnreachableInst(context, block);

        block = good;
        return GetNodeValue(Arg_, ctx, block);
        ;
    }
#endif

private:
    [[noreturn]] static void Throw(TEnsureWrapper const* thisPtr, TComputationContext* ctxPtr) {
        auto message = thisPtr->Message_->GetValue(*ctxPtr);
        auto messageStr = message.AsStringRef();
        TStringBuilder res;
        res << thisPtr->Pos_ << " Condition violated";
        if (messageStr.Size() > 0) {
            res << ":\n\n"
                << TStringBuf(messageStr) << "\n\n";
        }

        UdfTerminate(res.data());
    }

    void RegisterDependencies() const final {
        DependsOn(Arg_);
        DependsOn(Predicate_);
    }

    IComputationNode* const Arg_;
    IComputationNode* const Predicate_;
    IComputationNode* const Message_;
    const NUdf::TSourcePosition Pos_;
};

} // namespace

IComputationNode* WrapEnsure(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 6, "Expected 6 args");
    bool isOptional;
    auto unpackedType = UnpackOptionalData(callable.GetInput(1), isOptional);
    MKQL_ENSURE(unpackedType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    auto value = LocateNode(ctx.NodeLocator, callable, 0);
    auto predicate = LocateNode(ctx.NodeLocator, callable, 1);
    auto message = LocateNode(ctx.NodeLocator, callable, 2);
    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<ui32>();
    return new TEnsureWrapper(ctx.Mutables, value, predicate, message, NUdf::TSourcePosition(row, column, file));
}

} // namespace NKikimr::NMiniKQL
