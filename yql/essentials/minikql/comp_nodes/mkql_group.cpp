#include "mkql_group.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/compact_hash.h>
#include <yql/essentials/minikql/defs.h>

#include <util/generic/maybe.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool WithHandler>
class TGroupingCoreWrapper: public TMutableComputationNode<TGroupingCoreWrapper<WithHandler>> {
    using TSelf = TGroupingCoreWrapper;
    using TBaseComputation = TMutableComputationNode<TSelf>;

public:
    class TSplitStreamValue: public TComputationValue<TSplitStreamValue> {
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
            , CompCtx_(compCtx)
            , Self_(self)
            , Stream_(std::move(stream))
        {
        }

        NUdf::EFetchStatus NextKey(NUdf::TUnboxedValue& key) {
            if (Fetching == State_ || AtGroupStart == State_) {
                NUdf::EFetchStatus status = NUdf::EFetchStatus::Ok;
                for (NUdf::TUnboxedValue item; NUdf::EFetchStatus::Ok == status; status = Fetch(item)) {
                }
                if (NUdf::EFetchStatus::Finish != status) {
                    return status;
                }
            }

            if (Finished == State_) {
                return NUdf::EFetchStatus::Finish;
            }

            if (GroupFinished != State_) {
                auto status = Stream_.Fetch(Value_);
                if (NUdf::EFetchStatus::Finish == status) {
                    State_ = Finished;
                }
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }
            }

            Self_->KeyExtractorItemNode_->SetValue(CompCtx_, NUdf::TUnboxedValue(Value_));
            key = Self_->KeyExtractorResultNode_->GetValue(CompCtx_);
            Self_->GroupSwitchKeyNode_->SetValue(CompCtx_, NUdf::TUnboxedValue(key));
            Self_->GroupSwitchItemNode_->SetValue(CompCtx_, NUdf::TUnboxedValue(Value_));
            State_ = AtGroupStart;

            return NUdf::EFetchStatus::Ok;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Finished == State_) {
                return NUdf::EFetchStatus::Finish;
            }

            if (AtGroupStart != State_) {
                auto status = Stream_.Fetch(Value_);
                if (NUdf::EFetchStatus::Finish == status) {
                    State_ = Finished;
                }
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }
            }

            if (Fetching == State_) {
                Self_->GroupSwitchItemNode_->SetValue(CompCtx_, NUdf::TUnboxedValue(Value_));
                if (Self_->GroupSwitchResultNode_->GetValue(CompCtx_).template Get<bool>()) {
                    State_ = GroupFinished;
                    return NUdf::EFetchStatus::Finish;
                }
            } else {
                State_ = Fetching;
            }

            if constexpr (WithHandler) {
                Self_->HandlerItemNode_->SetValue(CompCtx_, std::move(Value_));
                result = Self_->HandlerResultNode_->GetValue(CompCtx_);
            } else {
                result = std::move(Value_);
            }
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx_;
        const TSelf* const Self_;
        NUdf::TUnboxedValue Stream_;
        EState State_ = AtStart;
        NUdf::TUnboxedValue Value_;
    };

    class TGroupStreamValue: public TComputationValue<TGroupStreamValue> {
    public:
        using TBase = TComputationValue<TGroupStreamValue>;

        TGroupStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const TSelf* self, NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , SplitStream_(CompCtx_.HolderFactory.Create<TSplitStreamValue>(CompCtx_, self, std::move(stream)))
            , SplitStreamValue_(static_cast<TSplitStreamValue*>(SplitStream_.AsBoxed().Get()))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            NUdf::TUnboxedValue key;
            auto status = SplitStreamValue_->NextKey(key);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            NKikimr::NUdf::TUnboxedValue* itemsPtr;
            result = CompCtx_.HolderFactory.CreateDirectArrayHolder(2, itemsPtr);
            itemsPtr[0] = std::move(key);
            itemsPtr[1] = SplitStream_;

            return status;
        }

        TComputationContext& CompCtx_;
        NUdf::TUnboxedValue SplitStream_;
        TSplitStreamValue* SplitStreamValue_;
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
        , Stream_(stream)
        , KeyExtractorItemNode_(keyExtractorItem)
        , KeyExtractorResultNode_(keyExtractorResult)
        , GroupSwitchKeyNode_(groupSwitchKey)
        , GroupSwitchItemNode_(groupSwitchItem)
        , GroupSwitchResultNode_(groupSwitchResult)
        , HandlerItemNode_(handlerItem)
        , HandlerResultNode_(handlerResult)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TGroupStreamValue>(ctx, this, Stream_->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        this->DependsOn(KeyExtractorResultNode_);
        this->DependsOn(GroupSwitchResultNode_);
        this->DependsOn(HandlerResultNode_);
        this->Own(KeyExtractorItemNode_);
        this->Own(GroupSwitchKeyNode_);
        this->Own(GroupSwitchItemNode_);
        this->Own(HandlerItemNode_);
    }

    IComputationNode* const Stream_;

    IComputationExternalNode* const KeyExtractorItemNode_;
    IComputationNode* const KeyExtractorResultNode_;

    IComputationExternalNode* const GroupSwitchKeyNode_;
    IComputationExternalNode* const GroupSwitchItemNode_;
    IComputationNode* const GroupSwitchResultNode_;

    IComputationExternalNode* const HandlerItemNode_;
    IComputationNode* const HandlerResultNode_;
};

} // namespace

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
        /*handlerItem=*/nullptr,
        /*handlerResult=*/nullptr);
}

} // namespace NKikimr::NMiniKQL
