#include "mkql_hopping.h"
#include "mkql_saveload.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/generic/scope.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr ui32 StateVersion = 1;
const TStatKey Hop_NewHopsCount("Hop_NewHopsCount", /*deriv=*/true);
const TStatKey Hop_ThrownEventsCount("Hop_ThrownEventsCount", /*deriv=*/true);

class THoppingCoreWrapper: public TMutableComputationNode<THoppingCoreWrapper> {
    using TBaseComputation = TMutableComputationNode<THoppingCoreWrapper>;

public:
    using TSelf = THoppingCoreWrapper;

    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue&& stream,
            const TSelf* self,
            ui64 hopTime,
            ui64 intervalHopCount,
            ui64 delayHopCount,
            TComputationContext& ctx)
            : TBase(memInfo)
            , Stream_(std::move(stream))
            , Self_(self)
            , HopTime_(hopTime)
            , IntervalHopCount_(intervalHopCount)
            , DelayHopCount_(delayHopCount)
            , Buckets_(IntervalHopCount_ + DelayHopCount_)
            , Ctx_(ctx)
        {
        }

    private:
        ui32 GetTraverseCount() const override {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
            Y_UNUSED(index);
            return Stream_;
        }

        NUdf::TUnboxedValue Save() const override {
            MKQL_ENSURE(Ready_.empty(), "Inconsistent state to save, not all elements are fetched");
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx_);

            out.Write<ui32>(Buckets_.size());
            for (const auto& bucket : Buckets_) {
                out(bucket.HasValue);
                if (bucket.HasValue) {
                    Self_->InSave_->SetValue(Ctx_, NUdf::TUnboxedValue(bucket.Value));
                    if (Self_->StateType_) {
                        out.WriteUnboxedValue(Self_->Packer_.RefMutableObject(Ctx_, false, Self_->StateType_), Self_->OutSave_->GetValue(Ctx_));
                    }
                }
            }

            out(HopIndex_, Started_, Finished_);
            return out.MakeState();
        }

        void Load(const NUdf::TStringRef& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);

            const auto loadStateVersion = in.GetStateVersion();
            if (loadStateVersion != StateVersion) {
                THROW yexception() << "Invalid state version " << loadStateVersion;
            }

            auto size = in.Read<ui32>();
            Buckets_.resize(size);
            for (auto& bucket : Buckets_) {
                bucket.HasValue = in.Read<bool>();
                if (bucket.HasValue) {
                    if (Self_->StateType_) {
                        Self_->InLoad_->SetValue(Ctx_, in.ReadUnboxedValue(Self_->Packer_.RefMutableObject(Ctx_, false, Self_->StateType_), Ctx_));
                    }
                    bucket.Value = Self_->OutLoad_->GetValue(Ctx_);
                }
            }

            in(HopIndex_, Started_, Finished_);
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Ready_.empty()) {
                result = std::move(Ready_.front());
                Ready_.pop_front();
                return NUdf::EFetchStatus::Ok;
            }
            if (Finished_) {
                return NUdf::EFetchStatus::Finish;
            }

            i64 thrownEvents = 0;
            i64 newHops = 0;
            Y_DEFER {
                if (thrownEvents) {
                    MKQL_ADD_STAT(Ctx_.Stats, Hop_ThrownEventsCount, thrownEvents);
                }
                if (newHops) {
                    MKQL_ADD_STAT(Ctx_.Stats, Hop_NewHopsCount, newHops);
                }
            };

            for (NUdf::TUnboxedValue item;;) {
                if (!Ready_.empty()) {
                    result = std::move(Ready_.front());
                    Ready_.pop_front();
                    return NUdf::EFetchStatus::Ok;
                }

                const auto status = Stream_.Fetch(item);
                if (status != NUdf::EFetchStatus::Ok) {
                    if (status == NUdf::EFetchStatus::Finish) {
                        Finished_ = true;
                    }
                    return status;
                }

                Self_->Item_->SetValue(Ctx_, std::move(item));
                auto time = Self_->OutTime_->GetValue(Ctx_);
                if (!time) {
                    continue;
                }

                auto hopIndex = time.Get<ui64>() / HopTime_;

                if (!Started_) {
                    HopIndex_ = hopIndex + 1;
                    Started_ = true;
                }

                while (hopIndex >= HopIndex_) {
                    auto firstBucketIndex = HopIndex_ % Buckets_.size();

                    auto bucketIndex = firstBucketIndex;
                    TMaybe<NUdf::TUnboxedValue> aggregated;

                    for (ui64 i = 0; i < IntervalHopCount_; ++i) {
                        const auto& bucket = Buckets_[bucketIndex];
                        if (bucket.HasValue) {
                            if (!aggregated) { // todo: clone
                                Self_->InSave_->SetValue(Ctx_, NUdf::TUnboxedValue(bucket.Value));
                                Self_->InLoad_->SetValue(Ctx_, Self_->OutSave_->GetValue(Ctx_));
                                aggregated = Self_->OutLoad_->GetValue(Ctx_);
                            } else {
                                Self_->State_->SetValue(Ctx_, NUdf::TUnboxedValue(bucket.Value));
                                Self_->State2_->SetValue(Ctx_, NUdf::TUnboxedValue(*aggregated));
                                aggregated = Self_->OutMerge_->GetValue(Ctx_);
                            }
                        }
                        if (++bucketIndex == Buckets_.size()) {
                            bucketIndex = 0;
                        }
                    }

                    auto& clearBucket = Buckets_[firstBucketIndex];
                    clearBucket.Value = NUdf::TUnboxedValue();
                    clearBucket.HasValue = false;

                    if (aggregated) {
                        Self_->State_->SetValue(Ctx_, NUdf::TUnboxedValue(*aggregated));
                        Self_->Time_->SetValue(Ctx_, NUdf::TUnboxedValuePod((HopIndex_ - DelayHopCount_) * HopTime_));
                        Ready_.emplace_back(Self_->OutFinish_->GetValue(Ctx_));
                    }

                    ++newHops;
                    ++HopIndex_;
                }

                if (hopIndex + DelayHopCount_ + 1 >= HopIndex_) {
                    auto& bucket = Buckets_[hopIndex % Buckets_.size()];
                    if (!bucket.HasValue) {
                        bucket.Value = Self_->OutInit_->GetValue(Ctx_);
                        bucket.HasValue = true;
                    } else {
                        Self_->State_->SetValue(Ctx_, NUdf::TUnboxedValue(bucket.Value));
                        bucket.Value = Self_->OutUpdate_->GetValue(Ctx_);
                    }
                } else {
                    ++thrownEvents;
                }
            }
        }

        const NUdf::TUnboxedValue Stream_;
        const TSelf* const Self_;

        const ui64 HopTime_;
        const ui64 IntervalHopCount_;
        const ui64 DelayHopCount_;

        struct TBucket {
            NUdf::TUnboxedValue Value;
            bool HasValue = false;
        };

        std::vector<TBucket> Buckets_;          // circular buffer
        std::deque<NUdf::TUnboxedValue> Ready_; // buffer for fetching results
        ui64 HopIndex_ = 0;
        bool Started_ = false;
        bool Finished_ = false;

        TComputationContext& Ctx_;
    };

    THoppingCoreWrapper(
        TComputationMutables& mutables,
        IComputationNode* stream,
        IComputationExternalNode* item,
        IComputationExternalNode* state,
        IComputationExternalNode* state2,
        IComputationExternalNode* time,
        IComputationExternalNode* inSave,
        IComputationExternalNode* inLoad,
        IComputationNode* outTime,
        IComputationNode* outInit,
        IComputationNode* outUpdate,
        IComputationNode* outSave,
        IComputationNode* outLoad,
        IComputationNode* outMerge,
        IComputationNode* outFinish,
        IComputationNode* hop,
        IComputationNode* interval,
        IComputationNode* delay,
        TType* stateType)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Item_(item)
        , State_(state)
        , State2_(state2)
        , Time_(time)
        , InSave_(inSave)
        , InLoad_(inLoad)
        , OutTime_(outTime)
        , OutInit_(outInit)
        , OutUpdate_(outUpdate)
        , OutSave_(outSave)
        , OutLoad_(outLoad)
        , OutMerge_(outMerge)
        , OutFinish_(outFinish)
        , Hop_(hop)
        , Interval_(interval)
        , Delay_(delay)
        , StateType_(stateType)
        , Packer_(mutables)
    {
        Stateless_ = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto hopTime = Hop_->GetValue(ctx).Get<ui64>();
        const auto interval = Interval_->GetValue(ctx).Get<ui64>();
        const auto delay = Delay_->GetValue(ctx).Get<ui64>();
        const auto intervalHopCount = interval / hopTime;
        const auto delayHopCount = delay / hopTime;

        return ctx.HolderFactory.Create<TStreamValue>(Stream_->GetValue(ctx), this, hopTime, intervalHopCount, delayHopCount, ctx);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);
        Own(Item_);
        Own(State_);
        Own(State2_);
        Own(Time_);
        Own(InSave_);
        Own(InLoad_);
        DependsOn(OutTime_);
        DependsOn(OutInit_);
        DependsOn(OutUpdate_);
        DependsOn(OutSave_);
        DependsOn(OutLoad_);
        DependsOn(OutMerge_);
        DependsOn(OutFinish_);
        DependsOn(Hop_);
        DependsOn(Interval_);
        DependsOn(Delay_);
    }

    IComputationNode* const Stream_;

    IComputationExternalNode* const Item_;
    IComputationExternalNode* const State_;
    IComputationExternalNode* const State2_;
    IComputationExternalNode* const Time_;
    IComputationExternalNode* const InSave_;
    IComputationExternalNode* const InLoad_;

    IComputationNode* const OutTime_;
    IComputationNode* const OutInit_;
    IComputationNode* const OutUpdate_;
    IComputationNode* const OutSave_;
    IComputationNode* const OutLoad_;
    IComputationNode* const OutMerge_;
    IComputationNode* const OutFinish_;

    IComputationNode* const Hop_;
    IComputationNode* const Interval_;
    IComputationNode* const Delay_;

    TType* const StateType_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer_;
};

} // namespace

IComputationNode* WrapHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 17, "Expected 17 args");

    auto hasSaveLoad = !callable.GetInput(10).GetStaticType()->IsVoid();

    IComputationExternalNode* inSave = nullptr;
    IComputationNode* outSave = nullptr;
    IComputationExternalNode* inLoad = nullptr;
    IComputationNode* outLoad = nullptr;

    auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    auto stream = LocateNode(ctx.NodeLocator, callable, 0);

    auto outTime = LocateNode(ctx.NodeLocator, callable, 7);
    auto outInit = LocateNode(ctx.NodeLocator, callable, 8);
    auto outUpdate = LocateNode(ctx.NodeLocator, callable, 9);
    if (hasSaveLoad) {
        outSave = LocateNode(ctx.NodeLocator, callable, 10);
        outLoad = LocateNode(ctx.NodeLocator, callable, 11);
    }
    auto outMerge = LocateNode(ctx.NodeLocator, callable, 12);
    auto outFinish = LocateNode(ctx.NodeLocator, callable, 13);

    auto hop = LocateNode(ctx.NodeLocator, callable, 14);
    auto interval = LocateNode(ctx.NodeLocator, callable, 15);
    auto delay = LocateNode(ctx.NodeLocator, callable, 16);

    auto item = LocateExternalNode(ctx.NodeLocator, callable, 1);
    auto state = LocateExternalNode(ctx.NodeLocator, callable, 2);
    auto state2 = LocateExternalNode(ctx.NodeLocator, callable, 3);
    auto time = LocateExternalNode(ctx.NodeLocator, callable, 4);
    if (hasSaveLoad) {
        inSave = LocateExternalNode(ctx.NodeLocator, callable, 5);
        inLoad = LocateExternalNode(ctx.NodeLocator, callable, 6);
    }

    auto stateType = hasSaveLoad ? callable.GetInput(10).GetStaticType() : nullptr;

    return new THoppingCoreWrapper(ctx.Mutables,
                                   stream, item, state, state2, time, inSave, inLoad,
                                   outTime, outInit, outUpdate, outSave, outLoad, outMerge, outFinish,
                                   hop, interval, delay, stateType);
}

} // namespace NKikimr::NMiniKQL
