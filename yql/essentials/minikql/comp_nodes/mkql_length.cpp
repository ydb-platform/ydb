#include "mkql_length.h"
#include <yql/essentials/utils/runtime_dispatch.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen.h>     // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsDict, bool IsOptional>
class TLengthWrapper: public TMutableCodegeneratorNode<TLengthWrapper<IsDict, IsOptional>> {
    typedef TMutableCodegeneratorNode<TLengthWrapper<IsDict, IsOptional>> TBaseComputation;

public:
    TLengthWrapper(TComputationMutables& mutables, IComputationNode* collection)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Collection(collection)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& collection = Collection->GetValue(compCtx);
        if (IsOptional && !collection) {
            return NUdf::TUnboxedValuePod();
        }
        const auto length = IsDict ? collection.GetDictLength() : collection.GetListLength();
        return NUdf::TUnboxedValuePod(length);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto collection = GetNodeValue(Collection, ctx, block);

        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(collection->getType(), 2U, "result", done);

            result->addIncoming(collection, block);
            BranchInst::Create(done, good, IsEmpty(collection, block, context), block);
            block = good;

            const auto length = CallBoxedValueVirtualMethod < IsDict ? NUdf::TBoxedValueAccessor::EMethod::GetDictLength : NUdf::TBoxedValueAccessor::EMethod::GetListLength > (Type::getInt64Ty(context), collection, ctx.Codegen, block);
            if (Collection->IsTemporaryValue()) {
                CleanupBoxed(collection, ctx, block);
            }
            result->addIncoming(SetterFor<ui64>(length, context, block), block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            const auto length = CallBoxedValueVirtualMethod < IsDict ? NUdf::TBoxedValueAccessor::EMethod::GetDictLength : NUdf::TBoxedValueAccessor::EMethod::GetListLength > (Type::getInt64Ty(context), collection, ctx.Codegen, block);
            if (Collection->IsTemporaryValue()) {
                CleanupBoxed(collection, ctx, block);
            }
            return SetterFor<ui64>(length, context, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Collection);
    }

    IComputationNode* const Collection;
};

} // namespace

IComputationNode* WrapLength(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto type = UnpackOptional(callable.GetInput(0).GetStaticType(), isOptional);
    const bool isDict = type->IsDict() || type->IsEmptyDict();
    const bool isList = type->IsList() || type->IsEmptyList();
    MKQL_ENSURE(isDict || isList, "Expected list or dict.");
    return YQL_RUNTIME_DISPATCH_NEW(IComputationNode*, TLengthWrapper, 2, isDict, isOptional, ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

} // namespace NMiniKQL
} // namespace NKikimr
