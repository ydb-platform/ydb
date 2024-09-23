#include "yql_splittype.h"
#include "yql_type_resource.h"
#include "yql_position.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

template <NYql::ETypeAnnotationKind Kind>
class TSplitTypeWrapper : public TMutableComputationNode<TSplitTypeWrapper<Kind>> {
    typedef TMutableComputationNode<TSplitTypeWrapper<Kind>> TBaseComputation;
public:
    TSplitTypeWrapper(TComputationMutables& mutables, IComputationNode* handle, ui32 exprCtxMutableIndex, NYql::TPosition pos)
        : TBaseComputation(mutables)
        , Handle_(handle)
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
        , Pos_(pos)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        auto handle = Handle_->GetValue(ctx);
        auto type = GetYqlType(handle);
        switch (Kind) {
        case NYql::ETypeAnnotationKind::Data: {
            auto castedType = type->UserCast<NYql::TDataExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* inplace = nullptr;
            if (auto params = dynamic_cast<const NYql::TDataExprParamsType*>(castedType)) {
                auto array = ctx.HolderFactory.CreateDirectArrayHolder(3, inplace);
                inplace[0] = MakeString(params->GetName());
                inplace[1] = MakeString(params->GetParamOne());
                inplace[2] = MakeString(params->GetParamTwo());
                return array;
            }

            auto array = ctx.HolderFactory.CreateDirectArrayHolder(1, inplace);
            inplace[0] = MakeString(castedType->GetName());
            return array;
        }

        case NYql::ETypeAnnotationKind::Optional: {
            auto castedType = type->UserCast<NYql::TOptionalExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetItemType()));
        }

        case NYql::ETypeAnnotationKind::List: {
            auto castedType = type->UserCast<NYql::TListExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetItemType()));
        }

        case NYql::ETypeAnnotationKind::Stream: {
            auto castedType = type->UserCast<NYql::TStreamExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetItemType()));
        }

        case NYql::ETypeAnnotationKind::Tuple: {
            auto castedType = type->UserCast<NYql::TTupleExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* inplace = nullptr;
            auto array = ctx.HolderFactory.CreateDirectArrayHolder(castedType->GetSize(), inplace);
            for (ui32 i = 0; i < castedType->GetSize(); ++i) {
                inplace[i] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetItems()[i]));
            }

            return array;
        }

        case NYql::ETypeAnnotationKind::Struct: {
            auto castedType = type->UserCast<NYql::TStructExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* inplace = nullptr;
            auto array = ctx.HolderFactory.CreateDirectArrayHolder(castedType->GetSize(), inplace);
            for (ui32 i = 0; i < castedType->GetSize(); ++i) {
                NUdf::TUnboxedValue memberType = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetItems()[i]->GetItemType()));
                NUdf::TUnboxedValue* pair = nullptr;
                inplace[i] = ctx.HolderFactory.CreateDirectArrayHolder(2, pair);
                pair[0] = MakeString(castedType->GetItems()[i]->GetName()); // Name
                pair[1] = memberType;                                       // Type
            }

            return array;
        }

        case NYql::ETypeAnnotationKind::Dict: {
            auto castedType = type->UserCast<NYql::TDictExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* pair = nullptr;
            auto array = ctx.HolderFactory.CreateDirectArrayHolder(2, pair);
            pair[0] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetKeyType())); // Key
            pair[1] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetPayloadType())); // Payload
            return array;
        }

        case NYql::ETypeAnnotationKind::Resource: {
            auto castedType = type->UserCast<NYql::TResourceExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return MakeString(castedType->GetTag());
        }

        case NYql::ETypeAnnotationKind::Tagged: {
            auto castedType = type->UserCast<NYql::TTaggedExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* pair = nullptr;
            auto array = ctx.HolderFactory.CreateDirectArrayHolder(2, pair);
            pair[0] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetBaseType())); // Base
            pair[1] = MakeString(castedType->GetTag()); // Tag
            return array;
        }

        case NYql::ETypeAnnotationKind::Variant: {
            auto castedType = type->UserCast<NYql::TVariantExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetUnderlyingType()));
        }

        case NYql::ETypeAnnotationKind::Callable: {
            auto castedType = type->UserCast<NYql::TCallableExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            NUdf::TUnboxedValue* inplace = nullptr;
            auto array = ctx.HolderFactory.CreateDirectArrayHolder(4, inplace);
            NUdf::TUnboxedValue* inplaceArgs = nullptr;
            NUdf::TUnboxedValue args = ctx.HolderFactory.CreateDirectArrayHolder(castedType->GetArgumentsSize(), inplaceArgs);
            for (ui32 i = 0; i < castedType->GetArgumentsSize(); ++i) {
                NUdf::TUnboxedValue* inplaceArg = nullptr;
                NUdf::TUnboxedValue arg = ctx.HolderFactory.CreateDirectArrayHolder(3, inplaceArg);
                auto flags = castedType->GetArguments()[i].Flags;
                NUdf::TUnboxedValue flagsList = ctx.HolderFactory.GetEmptyContainerLazy();
                if (flags & NUdf::ICallablePayload::TArgumentFlags::AutoMap) {
                    NUdf::TUnboxedValue* inplaceFlags = nullptr;
                    flagsList = ctx.HolderFactory.CreateDirectArrayHolder(1, inplaceFlags);
                    inplaceFlags[0] = MakeString(TStringBuf("AutoMap"));
                }

                inplaceArg[0] = flagsList; // Flags
                inplaceArg[1] = MakeString(castedType->GetArguments()[i].Name); // Name
                inplaceArg[2] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetArguments()[i].Type));; // Type
                inplaceArgs[i] = arg;
            }

            inplace[0] = args; // Arguments
            inplace[1] = NUdf::TUnboxedValuePod(ui32(castedType->GetOptionalArgumentsCount())); // OptionalArgumentsCount
            inplace[2] = MakeString(castedType->GetPayload()); // Payload
            inplace[3] = NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, castedType->GetReturnType()));; // Result
            return array;
        }

        case NYql::ETypeAnnotationKind::Pg: {
            auto castedType = type->UserCast<NYql::TPgExprType>(Pos_, *exprCtxPtr);
            if (!castedType) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            return MakeString(castedType->GetName());
        }

        default:
            MKQL_ENSURE(false, "Unsupported kind:" << Kind);
        }
    }

    void RegisterDependencies() const override {
        this->DependsOn(Handle_);
    }

private:
    IComputationNode* Handle_;
    ui32 ExprCtxMutableIndex_;
    NYql::TPosition Pos_;
};

template <NYql::ETypeAnnotationKind Kind>
IComputationNode* WrapSplitType(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 arg");
    auto pos = ExtractPosition(callable);
    auto handle = LocateNode(ctx.NodeLocator, callable, 3);
    return new TSplitTypeWrapper<Kind>(ctx.Mutables, handle, exprCtxMutableIndex, pos);
}

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Data>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Optional>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::List>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Stream>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Tuple>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Struct>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Dict>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Resource>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Tagged>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Variant>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Callable>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapSplitType<NYql::ETypeAnnotationKind::Pg>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

}
}
