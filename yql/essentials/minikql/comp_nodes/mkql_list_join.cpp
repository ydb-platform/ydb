#include "mkql_list_join.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <util/string/cast.h>

namespace NKikimr::NMiniKQL {

namespace {

using TColumnsMap = TVector<std::pair<ui32, ui32>>;

enum ETableIndex: ui32 {
    LeftIndex = 0U,
    RightIndex = 1U
};

class TListJoinCoreWrapper: public TMutableComputationNode<TListJoinCoreWrapper> {
    using TSelf = TListJoinCoreWrapper;
    using TBase = TMutableComputationNode<TSelf>;

public:
    TListJoinCoreWrapper(TComputationMutables& mutables, IComputationNode* stream, ui32 tableIndexField, TVector<bool>&& needUnwrap,
                         TColumnsMap&& keyColumns, TColumnsMap&& leftInputColumns, TColumnsMap&& rightInputColumns,
                         IComputationExternalNode* leftArgmapLambdaArg, IComputationNode* leftArgmapLambdaRoot,
                         IComputationExternalNode* rightArgmapLambdaArg, IComputationNode* rightArgmapLambdaRoot,
                         IComputationExternalNode* keyArg, IComputationExternalNode* leftListArg,
                         IComputationExternalNode* rightListArg, IComputationNode* joinResult)
        : TBase(mutables)
        , Stream_(stream)
        , TableIndexField_(tableIndexField)
        , NeedUnwrap_(std::move(needUnwrap))
        , KeyColumns_(std::move(keyColumns))
        , LeftColumns_(std::move(leftInputColumns))
        , RightColumns_(std::move(rightInputColumns))
        , LeftArgmapLambdaArg_(leftArgmapLambdaArg)
        , LeftArgmapLambdaRoot_(leftArgmapLambdaRoot)
        , RightArgmapLambdaArg_(rightArgmapLambdaArg)
        , RightArgmapLambdaRoot_(rightArgmapLambdaRoot)
        , KeyArg_(keyArg)
        , LeftListArg_(leftListArg)
        , RightListArg_(rightListArg)
        , JoinResult_(joinResult)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, std::move(Stream_->GetValue(ctx)),
                                                      TableIndexField_, KeyColumns_,
                                                      LeftColumns_, RightColumns_, NeedUnwrap_,
                                                      LeftArgmapLambdaArg_, LeftArgmapLambdaRoot_,
                                                      RightArgmapLambdaArg_, RightArgmapLambdaRoot_,
                                                      KeyArg_, LeftListArg_, RightListArg_, JoinResult_);
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                     NUdf::TUnboxedValue&& inputStreamValue, ui32 tableIndexField,
                     const TColumnsMap& keyColumns, const TColumnsMap& leftColumns,
                     const TColumnsMap& rightColumns, const TVector<bool>& needUnwrap,
                     IComputationExternalNode* leftArgmapLambdaArg, IComputationNode* leftArgmapLambdaRoot,
                     IComputationExternalNode* rightArgmapLambdaArg, IComputationNode* rightArgmapLambdaRoot,
                     IComputationExternalNode* keyArg, IComputationExternalNode* leftListArg,
                     IComputationExternalNode* rightListArg, IComputationNode* joinResult)
            : TBase(memInfo)
            , CompCtx_(ctx)
            , InputStreamValue_(std::move(inputStreamValue))
            , TableIndexField_(tableIndexField)
            , KeyColumns_(keyColumns)
            , LeftColumns_(leftColumns)
            , RightColumns_(rightColumns)
            , NeedUnwrap_(needUnwrap)
            , LeftArgmapLambdaArg_(leftArgmapLambdaArg)
            , LeftArgmapLambdaRoot_(leftArgmapLambdaRoot)
            , RightArgmapLambdaArg_(rightArgmapLambdaArg)
            , RightArgmapLambdaRoot_(rightArgmapLambdaRoot)
            , KeyArg_(keyArg)
            , LeftListArg_(leftListArg)
            , RightListArg_(rightListArg)
            , JoinResult_(joinResult)
            , KeyValue_(NUdf::TUnboxedValuePod::Invalid())
            , JoinedStreamValue_(NUdf::TUnboxedValuePod::Invalid())
        {
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            while (JoinedStreamValue_.IsInvalid()) {
                NUdf::TUnboxedValue value;
                switch (InputStreamValue_.Fetch(value)) {
                    case NUdf::EFetchStatus::Yield:
                        return NUdf::EFetchStatus::Yield;
                    case NUdf::EFetchStatus::Ok:
                        if (KeyValue_.IsInvalid()) {
                            KeyValue_ = InitializeKey(value);
                        }
                        switch (const auto tableIndex = value.GetElement(TableIndexField_).template Get<ui32>()) {
                            case LeftIndex:
                                LeftListValue_ = LeftListValue_.Append(MakeListItem(value, LeftColumns_, LeftArgmapLambdaArg_, LeftArgmapLambdaRoot_));
                                break;
                            case RightIndex:
                                RightListValue_ = RightListValue_.Append(MakeListItem(value, RightColumns_, RightArgmapLambdaArg_, RightArgmapLambdaRoot_));
                                break;
                            default:
                                THROW yexception() << "Bad table index: " << tableIndex;
                        };
                        break;
                    case NUdf::EFetchStatus::Finish:
                        // XXX: Handle empty stream input: if KeyValue_
                        // has not been initialized yet, no record was
                        // obtained from the input. Hence, simply yield
                        // the Finish status to the caller.
                        if (KeyValue_.IsInvalid()) {
                            return NUdf::EFetchStatus::Finish;
                        }
                        // Otherwise, call join lambda with the values,
                        // obtained earlier.
                        KeyArg_->SetValue(CompCtx_, std::move(KeyValue_));
                        LeftListArg_->SetValue(CompCtx_, CompCtx_.HolderFactory.CreateDirectListHolder(std::move(LeftListValue_)));
                        RightListArg_->SetValue(CompCtx_, CompCtx_.HolderFactory.CreateDirectListHolder(std::move(RightListValue_)));
                        JoinedStreamValue_ = std::move(JoinResult_->GetValue(CompCtx_));
                        break;
                }
            }

            return JoinedStreamValue_.Fetch(result);
        }

    private:
        NUdf::TUnboxedValue InitializeKey(const NUdf::TUnboxedValue& row) {
            if (KeyColumns_.size() == 1) {
                const auto keyIndex = KeyColumns_.front().first;
                return row.GetElement(keyIndex);
            }
            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto key = CompCtx_.HolderFactory.CreateDirectArrayHolder(KeyColumns_.size(), itemsPtr);
            if (const auto elements = row.GetElements()) {
                for (const auto [inColumn, outColumn] : KeyColumns_) {
                    itemsPtr[outColumn] = elements[inColumn];
                }
            } else {
                for (const auto [inColumn, outColumn] : KeyColumns_) {
                    itemsPtr[outColumn] = row.GetElement(inColumn);
                }
            }
            return key;
        }

        NUdf::TUnboxedValue MakeListItem(const NUdf::TUnboxedValue& row, const TColumnsMap& columns, IComputationExternalNode* arg, IComputationNode* value) {
            NUdf::TUnboxedValue* itemsPtr = nullptr;
            auto argValue = CompCtx_.HolderFactory.CreateDirectArrayHolder(columns.size(), itemsPtr);
            if (const auto elements = row.GetElements()) {
                for (const auto [inColumn, outColumn] : columns) {
                    auto item = elements[inColumn];
                    if (NeedUnwrap_[inColumn]) {
                        item = item.Release().GetOptionalValue();
                    }
                    itemsPtr[outColumn] = std::move(item);
                }
            } else {
                for (const auto [inColumn, outColumn] : columns) {
                    auto item = row.GetElement(inColumn);
                    if (NeedUnwrap_[inColumn]) {
                        item = item.Release().GetOptionalValue();
                    }
                    itemsPtr[outColumn] = std::move(item);
                }
            }
            arg->SetValue(CompCtx_, std::move(argValue));
            return value->GetValue(CompCtx_);
        }

        TComputationContext& CompCtx_;
        NUdf::TUnboxedValue InputStreamValue_;
        const ui32 TableIndexField_;
        const TColumnsMap& KeyColumns_;
        const TColumnsMap& LeftColumns_;
        const TColumnsMap& RightColumns_;
        const TVector<bool>& NeedUnwrap_;
        IComputationExternalNode* const LeftArgmapLambdaArg_;
        IComputationNode* const LeftArgmapLambdaRoot_;
        IComputationExternalNode* const RightArgmapLambdaArg_;
        IComputationNode* const RightArgmapLambdaRoot_;
        IComputationExternalNode* const KeyArg_;
        IComputationExternalNode* const LeftListArg_;
        IComputationExternalNode* const RightListArg_;
        IComputationNode* const JoinResult_;
        NUdf::TUnboxedValue KeyValue_;
        TDefaultListRepresentation LeftListValue_;
        TDefaultListRepresentation RightListValue_;
        NUdf::TUnboxedValue JoinedStreamValue_;
    };

    void RegisterDependencies() const final {
        DependsOn(Stream_);
        Own(LeftArgmapLambdaArg_);
        DependsOn(LeftArgmapLambdaRoot_);
        Own(RightArgmapLambdaArg_);
        DependsOn(RightArgmapLambdaRoot_);
        Own(KeyArg_);
        Own(LeftListArg_);
        Own(RightListArg_);
        DependsOn(JoinResult_);
    }

    IComputationNode* const Stream_;
    const ui32 TableIndexField_;
    const TVector<bool> NeedUnwrap_;
    const TColumnsMap KeyColumns_;
    const TColumnsMap LeftColumns_;
    const TColumnsMap RightColumns_;
    IComputationExternalNode* const LeftArgmapLambdaArg_;
    IComputationNode* const LeftArgmapLambdaRoot_;
    IComputationExternalNode* const RightArgmapLambdaArg_;
    IComputationNode* const RightArgmapLambdaRoot_;
    IComputationExternalNode* const KeyArg_;
    IComputationExternalNode* const LeftListArg_;
    IComputationExternalNode* const RightListArg_;
    IComputationNode* const JoinResult_;
};

TColumnsMap GetColumnsFromNodes(const TTupleLiteral* tuple) {
    TColumnsMap columns;
    columns.reserve(tuple->GetValuesCount());
    for (size_t i = 0; i < tuple->GetValuesCount(); i++) {
        const auto pair = AS_VALUE(TTupleLiteral, tuple->GetValue(i));
        const auto first = AS_VALUE(TDataLiteral, pair->GetValue(0))->AsValue().Get<ui32>();
        const auto second = AS_VALUE(TDataLiteral, pair->GetValue(1))->AsValue().Get<ui32>();
        columns.emplace_back(first, second);
    }
    return columns;
}

void EnrichNeedUnwrap(TVector<bool>& needUnwrap, const TColumnsMap& columns, const TStructType* inputType, const TStructType* outputType) {
    for (const auto [inColumn, outColumn] : columns) {
        const auto inType = inputType->GetMemberType(inColumn);
        const auto outType = outputType->GetMemberType(outColumn);
        if (outType->IsSameType(*inType)) {
            needUnwrap[inColumn] = false;
        } else if (inType->IsOptional() && outType->IsSameType(*AS_TYPE(TOptionalType, inType)->GetItemType())) {
            needUnwrap[inColumn] = true;
        } else {
            MKQL_ENSURE(false, "Bad column mapping: " << PrintNode(inType, true) << " -> " << PrintNode(outType, true));
        }
    }
}
} // namespace

IComputationNode* WrapListJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 12, "Expected 12 args");
    const auto inputType = callable.GetInput(0U).GetStaticType();
    MKQL_ENSURE(inputType->IsStream(), "Expected Stream as an input stream");
    const auto inputStreamType = AS_TYPE(TStreamType, inputType);
    MKQL_ENSURE(inputStreamType->GetItemType()->IsStruct(),
                "Expected Struct as an input item type");
    const auto inputStructType = AS_TYPE(TStructType, inputStreamType->GetItemType());
    const auto tableIndexField = AS_TYPE(TStructType, inputStructType)->GetMemberIndex("_yql_table_index");

    auto keyColumns = GetColumnsFromNodes(AS_VALUE(TTupleLiteral, callable.GetInput(1U)));
    auto leftColumns = GetColumnsFromNodes(AS_VALUE(TTupleLiteral, callable.GetInput(2U)));
    auto rightColumns = GetColumnsFromNodes(AS_VALUE(TTupleLiteral, callable.GetInput(3U)));

    const auto keyArgType = callable.GetInput(8U).GetStaticType();
    if (keyColumns.size() == 1) {
        const auto [inColumn, outColumn] = keyColumns.front();
        MKQL_ENSURE(inColumn < inputStructType->GetMembersCount(),
                    "Key column is out of bounds of input row");
        MKQL_ENSURE(outColumn == 0U, "Key column has to be 0 for the single key");
    } else {
        MKQL_ENSURE(keyArgType->IsStruct(), "Expected Struct as a key type");
        const auto keyStructType = AS_TYPE(TStructType, keyArgType);
        for (const auto [inColumn, outColumn] : keyColumns) {
            MKQL_ENSURE(inColumn < inputStructType->GetMembersCount(),
                        "Key column is out of input row bounds");
            MKQL_ENSURE(outColumn < keyStructType->GetMembersCount(),
                        "Key column is out of key structure bounds");
        }
    }

    const auto leftArgmapLambdaArgType = callable.GetInput(4U).GetStaticType();
    MKQL_ENSURE(leftArgmapLambdaArgType->IsStruct(), "Left argmap lambda argument must be a struct");
    const auto leftArgmapLambdaArgStructType = AS_TYPE(TStructType, leftArgmapLambdaArgType);
    for (const auto [inColumn, outColumn] : leftColumns) {
        MKQL_ENSURE(inColumn < inputStructType->GetMembersCount(),
                    "Left column is out of input row bounds");
        const auto inColumnType = inputStructType->GetMemberType(inColumn);
        MKQL_ENSURE(inColumnType->IsOptional() || inColumnType->IsPg(),
                    "Left payload has to be optional");
        MKQL_ENSURE(outColumn < leftArgmapLambdaArgStructType->GetMembersCount(),
                    "Left column is out of left argument structure bounds");
    }

    const auto rightArgmapLambdaArgType = callable.GetInput(6U).GetStaticType();
    MKQL_ENSURE(rightArgmapLambdaArgType->IsStruct(), "Right argmap lambda argument must be a struct");
    const auto rightArgmapLambdaArgStructType = AS_TYPE(TStructType, rightArgmapLambdaArgType);
    for (const auto [inColumn, outColumn] : rightColumns) {
        MKQL_ENSURE(inColumn < inputStructType->GetMembersCount(),
                    "Right column is out of input row bounds");
        const auto inColumnType = inputStructType->GetMemberType(inColumn);
        MKQL_ENSURE(inColumnType->IsOptional() || inColumnType->IsPg(),
                    "Right payload has to be optional");
        MKQL_ENSURE(outColumn < rightArgmapLambdaArgStructType->GetMembersCount(),
                    "Right column is out of right argument structure bounds");
    }

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected Stream as the output type");
    const auto joinResultType = callable.GetInput(11U).GetStaticType();
    MKQL_ENSURE(joinResultType->IsSameType(*joinType),
                "ListJoinCore output type must be the same as the join lambda type");

    TVector<bool> needUnwrap(inputStructType->GetMembersCount(), false);
    EnrichNeedUnwrap(needUnwrap, leftColumns, inputStructType, leftArgmapLambdaArgStructType);
    EnrichNeedUnwrap(needUnwrap, rightColumns, inputStructType, rightArgmapLambdaArgStructType);

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto leftArgmapLambdaRoot = LocateNode(ctx.NodeLocator, callable, 5U);
    const auto leftArgmapLambdaArg = LocateExternalNode(ctx.NodeLocator, callable, 4U);
    const auto rightArgmapLambdaRoot = LocateNode(ctx.NodeLocator, callable, 7U);
    const auto rightArgmapLambdaArg = LocateExternalNode(ctx.NodeLocator, callable, 6U);
    const auto keyArg = LocateExternalNode(ctx.NodeLocator, callable, 8U);
    const auto leftListArg = LocateExternalNode(ctx.NodeLocator, callable, 9U);
    const auto rightListArg = LocateExternalNode(ctx.NodeLocator, callable, 10U);
    const auto joinResult = LocateNode(ctx.NodeLocator, callable, 11U);
    return new TListJoinCoreWrapper(ctx.Mutables, stream, tableIndexField, std::move(needUnwrap),
                                    std::move(keyColumns), std::move(leftColumns), std::move(rightColumns),
                                    leftArgmapLambdaArg, leftArgmapLambdaRoot, rightArgmapLambdaArg, rightArgmapLambdaRoot,
                                    keyArg, leftListArg, rightListArg, joinResult);
}

} // namespace NKikimr::NMiniKQL
