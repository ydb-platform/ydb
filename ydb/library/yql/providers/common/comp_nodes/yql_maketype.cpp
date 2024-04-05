#include "yql_maketype.h"
#include "yql_type_resource.h"
#include "yql_position.h"
#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
    template <NYql::ETypeAnnotationKind Kind>
    struct TMakeTypeArgs;

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Data> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Optional> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::List> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Stream> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Tuple> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Struct> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Dict> {
        static constexpr size_t MinValue = 2;
        static constexpr size_t MaxValue = 2;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Resource> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Tagged> {
        static constexpr size_t MinValue = 2;
        static constexpr size_t MaxValue = 2;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Variant> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Void> {
        static constexpr size_t MinValue = 0;
        static constexpr size_t MaxValue = 0;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Null> {
        static constexpr size_t MinValue = 0;
        static constexpr size_t MaxValue = 0;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::EmptyList> {
        static constexpr size_t MinValue = 0;
        static constexpr size_t MaxValue = 0;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::EmptyDict> {
        static constexpr size_t MinValue = 0;
        static constexpr size_t MaxValue = 0;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Callable> {
        static constexpr size_t MinValue = 2;
        static constexpr size_t MaxValue = 4;
    };

    template <>
    struct TMakeTypeArgs<NYql::ETypeAnnotationKind::Pg> {
        static constexpr size_t MinValue = 1;
        static constexpr size_t MaxValue = 1;
    };
}

template <NYql::ETypeAnnotationKind Kind>
class TMakeTypeWrapper : public TMutableComputationNode<TMakeTypeWrapper<Kind>> {
    typedef TMutableComputationNode<TMakeTypeWrapper<Kind>> TBaseComputation;
public:
    TMakeTypeWrapper(TComputationMutables& mutables, TVector<IComputationNode*>&& args, ui32 exprCtxMutableIndex, NYql::TPosition pos)
        : TBaseComputation(mutables)
        , Args_(std::move(args))
        , ExprCtxMutableIndex_(exprCtxMutableIndex)
        , Pos_(pos)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto exprCtxPtr = GetExprContextPtr(ctx, ExprCtxMutableIndex_);
        const NYql::TTypeAnnotationNode* retType = nullptr;
        switch (Kind) {
        case NYql::ETypeAnnotationKind::Data: {
            auto iter = Args_[0]->GetValue(ctx).GetListIterator();
            NUdf::TUnboxedValue str;
            MKQL_ENSURE(iter.Next(str), "Unexpected end of list");
            auto typeName = TStringBuf(str.AsStringRef());
            auto slot = NUdf::FindDataSlot(typeName);
            if (!slot) {
                UdfTerminate((TStringBuilder() << Pos_ << ": Unknown data type: " << typeName).data());
            }

            if (*slot == NUdf::EDataSlot::Decimal) {
                NUdf::TUnboxedValue param1, param2;
                MKQL_ENSURE(iter.Next(param1), "Unexpected end of list");
                MKQL_ENSURE(iter.Next(param2), "Unexpected end of list");
                auto dataType = exprCtxPtr->template MakeType<NYql::TDataExprParamsType>(*slot, TStringBuf(param1.AsStringRef()), TStringBuf(param2.AsStringRef()));
                if (!dataType->Validate(Pos_, *exprCtxPtr)) {
                    UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
                }

                retType = dataType;
            } else {
                retType = exprCtxPtr->template MakeType<NYql::TDataExprType>(*slot);
            }

            MKQL_ENSURE(!iter.Skip(), "Too many elements in list");
            break;
        }

        case NYql::ETypeAnnotationKind::Optional: {
            auto type = GetYqlType(Args_[0]->GetValue(ctx));
            retType = exprCtxPtr->template MakeType<NYql::TOptionalExprType>(type);
            break;
        }

        case NYql::ETypeAnnotationKind::List: {
            auto type = GetYqlType(Args_[0]->GetValue(ctx));
            retType = exprCtxPtr->template MakeType<NYql::TListExprType>(type);
            break;
        }

        case NYql::ETypeAnnotationKind::Stream: {
            auto type = GetYqlType(Args_[0]->GetValue(ctx));
            retType = exprCtxPtr->template MakeType<NYql::TStreamExprType>(type);
            break;
        }

        case NYql::ETypeAnnotationKind::Tuple: {
            auto iter = Args_[0]->GetValue(ctx).GetListIterator();
            NYql::TTypeAnnotationNode::TListType items;
            NUdf::TUnboxedValue value;
            while (iter.Next(value)) {
                items.push_back(GetYqlType(value));
            }

            auto tupleType = exprCtxPtr->template MakeType<NYql::TTupleExprType>(items);
            if (!tupleType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = tupleType;
            break;
        }

        case NYql::ETypeAnnotationKind::Struct: {
            auto iter = Args_[0]->GetValue(ctx).GetListIterator();
            TVector<const NYql::TItemExprType*> items;
            NUdf::TUnboxedValue value;
            while (iter.Next(value)) {
                auto nameValue = value.GetElement(0); // Name
                auto typeValue = value.GetElement(1); // Value
                items.push_back(exprCtxPtr->template MakeType<NYql::TItemExprType>(TStringBuf(nameValue.AsStringRef()), GetYqlType(typeValue)));
            }

            auto structType = exprCtxPtr->template MakeType<NYql::TStructExprType>(items);
            if (!structType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = structType;
            break;
        }

        case NYql::ETypeAnnotationKind::Dict: {
            auto keyType = GetYqlType(Args_[0]->GetValue(ctx));
            auto payloadType = GetYqlType(Args_[1]->GetValue(ctx));
            auto dictType = exprCtxPtr->template MakeType<NYql::TDictExprType>(keyType, payloadType);
            if (!dictType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = dictType;
            break;
        }

        case NYql::ETypeAnnotationKind::Resource: {
            auto tagValue = Args_[0]->GetValue(ctx);
            retType = exprCtxPtr->template MakeType<NYql::TResourceExprType>(tagValue.AsStringRef());
            break;
        }

        case NYql::ETypeAnnotationKind::Tagged: {
            auto baseType = GetYqlType(Args_[0]->GetValue(ctx));
            auto tagValue = Args_[1]->GetValue(ctx);
            auto tagType = exprCtxPtr->template MakeType<NYql::TTaggedExprType>(baseType, tagValue.AsStringRef());
            if (!tagType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = tagType;
            break;
        }

        case NYql::ETypeAnnotationKind::Variant: {
            auto type = GetYqlType(Args_[0]->GetValue(ctx));
            auto varType = exprCtxPtr->template MakeType<NYql::TVariantExprType>(type);
            if (!varType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = varType;
            break;
        }

        case NYql::ETypeAnnotationKind::Void: {
            retType = exprCtxPtr->template MakeType<NYql::TVoidExprType>();
            break;
        }

        case NYql::ETypeAnnotationKind::Null: {
            retType = exprCtxPtr->template MakeType<NYql::TNullExprType>();
            break;
        }

        case NYql::ETypeAnnotationKind::EmptyList: {
            retType = exprCtxPtr->template MakeType<NYql::TEmptyListExprType>();
            break;
        }

        case NYql::ETypeAnnotationKind::EmptyDict: {
            retType = exprCtxPtr->template MakeType<NYql::TEmptyDictExprType>();
            break;
        }

        case NYql::ETypeAnnotationKind::Callable: {
            auto resultType = GetYqlType(Args_[0]->GetValue(ctx));
            auto argsValue = Args_[1]->GetValue(ctx);
            TVector<NYql::TCallableExprType::TArgumentInfo> args;
            NUdf::TUnboxedValue argValue;
            auto argsIterator = argsValue.GetListIterator();
            while (argsIterator.Next(argValue)) {
                auto flagsValue = argValue.GetElement(0); // Flags
                auto nameValue = argValue.GetElement(1); // Name
                auto typeValue = argValue.GetElement(2); // Type
                NYql::TCallableExprType::TArgumentInfo info;
                info.Flags = 0;
                auto flagsIterator = flagsValue.GetListIterator();
                NUdf::TUnboxedValue flagValue;
                while (flagsIterator.Next(flagValue)) {
                    auto flagName = TStringBuf(flagValue.AsStringRef());
                    if (flagName == TStringBuf("AutoMap")) {
                        info.Flags |= NUdf::ICallablePayload::TArgumentFlags::AutoMap;
                    } else {
                        UdfTerminate((TStringBuilder() << Pos_ << ": Unknown flag: " << flagName << ", known flags: AutoMap.").data());
                    }
                }

                info.Name = exprCtxPtr->AppendString(nameValue.AsStringRef());
                info.Type = GetYqlType(typeValue);
                args.push_back(info);
            }

            auto optCountValue = Args_.size() > 2 ? Args_[2]->GetValue(ctx) : NUdf::TUnboxedValue();
            auto payloadValue = Args_.size() > 3 ? Args_[3]->GetValue(ctx) : NUdf::TUnboxedValue();
            auto optCount = optCountValue ? optCountValue.template Get<ui32>() : 0;
            auto payload = payloadValue ? TStringBuf(payloadValue.AsStringRef()) : TStringBuf("");
            auto callableType = exprCtxPtr->template MakeType<NYql::TCallableExprType>(resultType, args, optCount, payload);
            if (!callableType->Validate(Pos_, *exprCtxPtr)) {
                UdfTerminate(exprCtxPtr->IssueManager.GetIssues().ToString().data());
            }

            retType = callableType;
            break;
        }

        case NYql::ETypeAnnotationKind::Pg: {
            auto name = Args_[0]->GetValue(ctx);
            auto nameStr = TString(name.AsStringRef());
            if (!NYql::NPg::HasType(nameStr)) {
                UdfTerminate((TStringBuilder() << Pos_ << ": Unknown Pg type: " << nameStr).data());
            }

            auto pgType = exprCtxPtr->template MakeType<NYql::TPgExprType>(NYql::NPg::LookupType(nameStr).TypeId);
            retType = pgType;
            break;
        }

        default:
            MKQL_ENSURE(false, "Unsupported kind:" << Kind);
        }

        return NUdf::TUnboxedValuePod(new TYqlTypeResource(exprCtxPtr, retType));
    }

    void RegisterDependencies() const override {
        for (auto arg : Args_) {
            this->DependsOn(arg);
        }
    }

private:
    TVector<IComputationNode*> Args_;
    ui32 ExprCtxMutableIndex_;
    NYql::TPosition Pos_;
};

template <NYql::ETypeAnnotationKind Kind>
IComputationNode* WrapMakeType(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    constexpr size_t expectedMinArgsCount = TMakeTypeArgs<Kind>::MinValue + 3;
    constexpr size_t expectedMaxArgsCount = TMakeTypeArgs<Kind>::MaxValue + 3;
    MKQL_ENSURE(callable.GetInputsCount() >= expectedMinArgsCount
        && callable.GetInputsCount() <= expectedMaxArgsCount, "Expected from " << expectedMinArgsCount << " to "
        << expectedMaxArgsCount << " arg(s), but got: " << callable.GetInputsCount());
    auto pos = ExtractPosition(callable);
    TVector<IComputationNode*> args;
    args.reserve(callable.GetInputsCount() - 3);
    for (size_t i = 0; i < callable.GetInputsCount() - 3; ++i) {
        args.push_back(LocateNode(ctx.NodeLocator, callable, i + 3));
    }

    return new TMakeTypeWrapper<Kind>(ctx.Mutables, std::move(args), exprCtxMutableIndex, pos);
}

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Data>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Optional>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::List>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Stream>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Tuple>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Struct>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Dict>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Resource>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Tagged>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Variant>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Void>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Null>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::EmptyList>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::EmptyDict>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Callable>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

template IComputationNode* WrapMakeType<NYql::ETypeAnnotationKind::Pg>
    (TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

}
}
