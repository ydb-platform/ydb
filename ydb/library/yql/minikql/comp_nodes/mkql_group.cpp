#include "mkql_group.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/compact_hash.h>
#include <ydb/library/yql/minikql/defs.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool WithHandler>
class TGroupingCoreWrapper: public TMutableComputationNode<TGroupingCoreWrapper<WithHandler>> {
    using TSelf = TGroupingCoreWrapper;
    typedef TMutableComputationNode<TSelf> TBaseComputation;
public:

    class TSplitStreamValue : public TComputationValue<TSplitStreamValue> {
    public:
        using TBase = TComputationValue<TSplitStreamValue>;

        enum EState {
            AtStart,
            AtGroupStart,
            Fetching,
            GroupFinished,
            Finished,
        };

        TSplitStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const TSelf* self, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Self(self)
            , Stream(std::move(stream))
        {
        }

        NUdf::EFetchStatus NextKey(NUdf::TUnboxedValue& key) {
            if (Fetching == State || AtGroupStart == State) {
                NUdf::EFetchStatus status = NUdf::EFetchStatus::Ok;
                for (NUdf::TUnboxedValue item; NUdf::EFetchStatus::Ok == status; status = Fetch(item)) {
                }
                if (NUdf::EFetchStatus::Finish != status) {
                    return status;
                }
            }

            if (Finished == State) {
                return NUdf::EFetchStatus::Finish;
            }

            if (GroupFinished != State) {
                auto status = Stream.Fetch(Value);
                if (NUdf::EFetchStatus::Finish == status) {
                    State = Finished;
                }
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }
            }

            Self->KeyExtractorItemNode->SetValue(CompCtx, NUdf::TUnboxedValue(Value));
            key = Self->KeyExtractorResultNode->GetValue(CompCtx);
            Self->GroupSwitchKeyNode->SetValue(CompCtx, NUdf::TUnboxedValue(key));
            Self->GroupSwitchItemNode->SetValue(CompCtx, NUdf::TUnboxedValue(Value));
            State = AtGroupStart;

            return NUdf::EFetchStatus::Ok;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished == State) {
                return NUdf::EFetchStatus::Finish;
            }

            if (AtGroupStart != State) {
                auto status = Stream.Fetch(Value);
                if (NUdf::EFetchStatus::Finish == status) {
                    State = Finished;
                }
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }
            }

            if (Fetching == State) {
                Self->GroupSwitchItemNode->SetValue(CompCtx, NUdf::TUnboxedValue(Value));
                if (Self->GroupSwitchResultNode->GetValue(CompCtx).template Get<bool>()) {
                    State = GroupFinished;
                    return NUdf::EFetchStatus::Finish;
                }
            } else {
                State = Fetching;
            }

            if constexpr (WithHandler) {
                Self->HandlerItemNode->SetValue(CompCtx, std::move(Value));
                result = Self->HandlerResultNode->GetValue(CompCtx);
            } else {
                result = std::move(Value);
            }
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        const TSelf* const Self;
        NUdf::TUnboxedValue Stream;
        EState State = AtStart;
        NUdf::TUnboxedValue Value;
    };

    class TGroupStreamValue : public TComputationValue<TGroupStreamValue> {
    public:
        using TBase = TComputationValue<TGroupStreamValue>;

        TGroupStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const TSelf* self, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , SplitStream(CompCtx.HolderFactory.Create<TSplitStreamValue>(CompCtx, self, std::move(stream)))
            , SplitStreamValue(static_cast<TSplitStreamValue*>(SplitStream.AsBoxed().Get()))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            NUdf::TUnboxedValue key;
            auto status = SplitStreamValue->NextKey(key);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            NKikimr::NUdf::TUnboxedValue* itemsPtr;
            result = CompCtx.HolderFactory.CreateDirectArrayHolder(2, itemsPtr);
            itemsPtr[0] = std::move(key);
            itemsPtr[1] = SplitStream;

            return status;
        }

    private:
        TComputationContext& CompCtx;
        NUdf::TUnboxedValue SplitStream;
        TSplitStreamValue* SplitStreamValue;
    };

    TGroupingCoreWrapper(TComputationMutables& mutables,
        IComputationNode* stream,
        IComputationExternalNode* keyExtractorItem,
        IComputationNode* keyExtractorResult,
        IComputationExternalNode* groupSwitchKey,
        IComputationExternalNode* groupSwitchItem,
        IComputationNode* groupSwitchResult,
        IComputationExternalNode* handlerItem,
        IComputationNode* handlerResult)
        : TBaseComputation(mutables)
        , Stream(stream)
        , KeyExtractorItemNode(keyExtractorItem)
        , KeyExtractorResultNode(keyExtractorResult)
        , GroupSwitchKeyNode(groupSwitchKey)
        , GroupSwitchItemNode(groupSwitchItem)
        , GroupSwitchResultNode(groupSwitchResult)
        , HandlerItemNode(handlerItem)
        , HandlerResultNode(handlerResult)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TGroupStreamValue>(ctx, this, Stream->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
        this->DependsOn(KeyExtractorResultNode);
        this->DependsOn(GroupSwitchResultNode);
        this->DependsOn(HandlerResultNode);
        this->Own(KeyExtractorItemNode);
        this->Own(GroupSwitchKeyNode);
        this->Own(GroupSwitchItemNode);
        this->Own(HandlerItemNode);
    }

private:
    IComputationNode* const Stream;

    IComputationExternalNode* const KeyExtractorItemNode;
    IComputationNode* const KeyExtractorResultNode;

    IComputationExternalNode* const GroupSwitchKeyNode;
    IComputationExternalNode* const GroupSwitchItemNode;
    IComputationNode* const GroupSwitchResultNode;

    IComputationExternalNode* const HandlerItemNode;
    IComputationNode* const HandlerResultNode;
};

}

IComputationNode* WrapGroupingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 6 || callable.GetInputsCount() == 8, "Expected 6 or 8 args");

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto keyExtractorResult = LocateNode(ctx.NodeLocator, callable, 1);
    const auto groupSwitchResult = LocateNode(ctx.NodeLocator, callable, 2);
    const auto keyExtractorItem = LocateExternalNode(ctx.NodeLocator, callable, 3);
    const auto groupSwitchKey = LocateExternalNode(ctx.NodeLocator, callable, 4);
    const auto groupSwitchItem = LocateExternalNode(ctx.NodeLocator, callable, 5);

    if (callable.GetInputsCount() == 8) {
        auto handlerResult = LocateNode(ctx.NodeLocator, callable, 6);
        auto handlerItem = LocateExternalNode(ctx.NodeLocator, callable, 7);
        return new TGroupingCoreWrapper<true>(
            ctx.Mutables,
            stream,
            keyExtractorItem,
            keyExtractorResult,
            groupSwitchKey,
            groupSwitchItem,
            groupSwitchResult,
            handlerItem,
            handlerResult);
    }

    return new TGroupingCoreWrapper<false>(
        ctx.Mutables,
        stream,
        keyExtractorItem,
        keyExtractorResult,
        groupSwitchKey,
        groupSwitchItem,
        groupSwitchResult,
        nullptr,
        nullptr);
}

}
}
