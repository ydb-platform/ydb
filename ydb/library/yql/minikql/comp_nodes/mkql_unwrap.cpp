#include "mkql_unwrap.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TUnwrapWrapper : public TDecoratorCodegeneratorNode<TUnwrapWrapper> {
    typedef TDecoratorCodegeneratorNode<TUnwrapWrapper> TBaseComputation;
public:
    TUnwrapWrapper(IComputationNode* optional, IComputationNode* message, const NUdf::TSourcePosition& pos)
        : TBaseComputation(optional)
        , Message(message)
        , Pos(pos)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx, const NUdf::TUnboxedValuePod& value) const {
        if (value) {
            return value.GetOptionalValue();
        }

        Throw(this, &compCtx);
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(kill, good, IsEmpty(value, block), block);

        block = kill;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TUnwrapWrapper::Throw));
        const auto doFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);
        const auto doFuncType = FunctionType::get(Type::getVoidTy(context), { Type::getInt64Ty(context), ctx.Ctx->getType() }, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
        CallInst::Create(doFuncType, doFuncPtr, { doFuncArg, ctx.Ctx }, "", block)->setTailCall();
        new UnreachableInst(context, block);

        block = good;
        return GetOptionalValue(context, value, block);
    }
#endif
private:
    [[noreturn]] static void Throw(TUnwrapWrapper const* thisPtr, TComputationContext* ctxPtr) {
        auto message = thisPtr->Message->GetValue(*ctxPtr);
        auto messageStr = message.AsStringRef();
        TStringBuilder res;
        res << thisPtr->Pos << " Failed to unwrap empty optional";
        if (messageStr.Size() > 0) {
            res << ":\n\n" << TStringBuf(messageStr) << "\n\n";
        }

        UdfTerminate(res.data());
    }

    IComputationNode* const Message;
    const NUdf::TSourcePosition Pos;
};

}

IComputationNode* WrapUnwrap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();

    return new TUnwrapWrapper(LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1),
        NUdf::TSourcePosition(row, column, file));
}

}
}
