#include "mkql_ensure.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TEnsureWrapper : public TMutableCodegeneratorNode<TEnsureWrapper> {
    typedef TMutableCodegeneratorNode<TEnsureWrapper> TBaseComputation;
public:
    TEnsureWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* predicate,
        IComputationNode* message, const NUdf::TSourcePosition& pos)
        : TBaseComputation(mutables, value->GetRepresentation())
        , Arg(value)
        , Predicate(predicate)
        , Message(message)
        , Pos(pos)
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
        auto& context = ctx.Codegen.GetContext();

        const auto predicate = GetNodeValue(Predicate, ctx, block);
        const auto pass = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(good, kill, pass, block);

        block = kill;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TEnsureWrapper::Throw));
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
    [[noreturn]] static void Throw(TEnsureWrapper const* thisPtr, TComputationContext* ctxPtr) {
        auto message = thisPtr->Message->GetValue(*ctxPtr);
        auto messageStr = message.AsStringRef();
        TStringBuilder res;
        res << thisPtr->Pos << " Condition violated";
        if (messageStr.Size() > 0) {
            res << ":\n\n" << TStringBuf(messageStr) << "\n\n";
        }

        UdfTerminate(res.data());
    }

    void RegisterDependencies() const final {
        DependsOn(Arg);
        DependsOn(Predicate);
    }

    IComputationNode* const Arg;
    IComputationNode* const Predicate;
    IComputationNode* const Message;
    const NUdf::TSourcePosition Pos;
};

}

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

}
}
