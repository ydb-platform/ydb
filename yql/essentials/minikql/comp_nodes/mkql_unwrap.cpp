#include "mkql_unwrap.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/udf/udf_terminator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TUnwrapWrapper: public TBinaryCodegeneratorNode<TUnwrapWrapper> {
    typedef TBinaryCodegeneratorNode<TUnwrapWrapper> TBaseComputation;

public:
    TUnwrapWrapper(IComputationNode* optional, IComputationNode* message, const NUdf::TSourcePosition& pos, EValueRepresentation kind)
        : TBaseComputation(optional, message, kind)
        , Pos(pos) {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Optional()->GetValue(compCtx);
        if (value) {
            return value.GetOptionalValue();
        }

        Throw(this, &compCtx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        Value* value = GetNodeValue(Optional(), ctx, block);
        auto& context = ctx.Codegen.GetContext();

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(kill, good, IsEmpty(value, block, context), block);

        block = kill;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TUnwrapWrapper::Throw));
        const auto doFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);
        const auto doFuncType = FunctionType::get(Type::getVoidTy(context), {Type::getInt64Ty(context), ctx.Ctx->getType()}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
        CallInst::Create(doFuncType, doFuncPtr, {doFuncArg, ctx.Ctx}, "", block)->setTailCall();
        new UnreachableInst(context, block);

        block = good;
        return GetOptionalValue(context, value, block);
    }
#endif
private:
    [[noreturn]] static void Throw(TUnwrapWrapper const* self, TComputationContext* ctx) {
        auto message = self->Message()->GetValue(*ctx);
        MKQL_ENSURE(message.IsString() || message.IsEmbedded(), "Message must be represented as a string.");
        TStringBuilder res;
        res << self->Pos << " Failed to unwrap empty optional";
        if (message.AsStringRef().Size() > 0) {
            res << ":\n\n"
                << TStringBuf(message.AsStringRef()) << "\n\n";
        }

        UdfTerminate(res.data());
    }

    IComputationNode* Optional() const {
        return Left;
    };

    IComputationNode* Message() const {
        return Right;
    };

    const NUdf::TSourcePosition Pos;
};
} // namespace

IComputationNode* WrapUnwrap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();
    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());
    return new TUnwrapWrapper(LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1),
                              NUdf::TSourcePosition(row, column, file), kind);
}
} // namespace NMiniKQL
} // namespace NKikimr
