#include "mkql_hopping.h"
#include "mkql_saveload.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

constexpr ui32 StateVersion = 1;
const TStatKey Hop_NewHopsCount("Hop_NewHopsCount", true);
const TStatKey Hop_ThrownEventsCount("Hop_ThrownEventsCount", true);

class THoppingCoreWrapper : public TMutableComputationNode<THoppingCoreWrapper> {
    typedef TMutableComputationNode<THoppingCoreWrapper> TBaseComputation;
public:
    using TSelf = THoppingCoreWrapper;

    class TStreamValue : public TComputationValue<TStreamValue> {
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
            , Stream(std::move(stream))
            , Self(self)
            , HopTime(hopTime)
            , IntervalHopCount(intervalHopCount)
            , DelayHopCount(delayHopCount)
            , Buckets(IntervalHopCount + DelayHopCount)
            , Ctx(ctx)
        {}

    private:
        ui32 GetTraverseCount() const override {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::TUnboxedValue Save() const override {
            MKQL_ENSURE(Ready.empty(), "Inconsistent state to save, not all elements are fetched");
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);

            out.Write<ui32>(Buckets.size());
            for (const auto& bucket : Buckets) {
                out(bucket.HasValue);
                if (bucket.HasValue) {
                    Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                    if (Self->StateType) {
                        out.WriteUnboxedValue(Self->Packer.RefMutableObject(Ctx, false, Self->StateType), Self->OutSave->GetValue(Ctx));
                    }
                }
            }

            out(HopIndex, Started, Finished);
            return out.MakeState();
        }

        void Load(const NUdf::TStringRef& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);

            const auto loadStateVersion = in.GetStateVersion();
            if (loadStateVersion != StateVersion) {
                THROW yexception() << "Invalid state version " << loadStateVersion;
            }

            auto size = in.Read<ui32>();
            Buckets.resize(size);
            for (auto& bucket : Buckets) {
                bucket.HasValue = in.Read<bool>();
                if (bucket.HasValue) {
                    if (Self->StateType) {
                        Self->InLoad->SetValue(Ctx, in.ReadUnboxedValue(Self->Packer.RefMutableObject(Ctx, false, Self->StateType), Ctx));
                    }
                    bucket.Value = Self->OutLoad->GetValue(Ctx);
                }
            }

            in(HopIndex, Started, Finished);
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Ready.empty()) {
                result = std::move(Ready.front());
                Ready.pop_front();
                return NUdf::EFetchStatus::Ok;
            }
            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            i64 thrownEvents = 0;
            i64 newHops = 0;
            Y_DEFER {
                if (thrownEvents) {
                    MKQL_ADD_STAT(Ctx.Stats, Hop_ThrownEventsCount, thrownEvents);
                }
                if (newHops) {
                    MKQL_ADD_STAT(Ctx.Stats, Hop_NewHopsCount, newHops);
                }
            };

            for (NUdf::TUnboxedValue item;;) {
                if (!Ready.empty()) {
                    result = std::move(Ready.front());
                    Ready.pop_front();
                    return NUdf::EFetchStatus::Ok;
                }

                const auto status = Stream.Fetch(item);
                if (status != NUdf::EFetchStatus::Ok) {
                    if (status == NUdf::EFetchStatus::Finish) {
                        Finished = true;
                    }
                    return status;
                }

                Self->Item->SetValue(Ctx, std::move(item));
                auto time = Self->OutTime->GetValue(Ctx);
                if (!time) {
                    continue;
                }

                auto hopIndex = time.Get<ui64>() / HopTime;

                if (!Started) {
                    HopIndex = hopIndex + 1;
                    Started = true;
                }

                while (hopIndex >= HopIndex) {
                    auto firstBucketIndex = HopIndex % Buckets.size();

                    auto bucketIndex = firstBucketIndex;
                    TMaybe<NUdf::TUnboxedValue> aggregated;

                    for (ui64 i = 0; i < IntervalHopCount; ++i) {
                        const auto& bucket = Buckets[bucketIndex];
                        if (bucket.HasValue) {
                            if (!aggregated) { // todo: clone
                                Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                                Self->InLoad->SetValue(Ctx, Self->OutSave->GetValue(Ctx));
                                aggregated = Self->OutLoad->GetValue(Ctx);
                            } else {
                                Self->State->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                                Self->State2->SetValue(Ctx, NUdf::TUnboxedValue(*aggregated));
                                aggregated = Self->OutMerge->GetValue(Ctx);
                            }
                        }
                        if (++bucketIndex == Buckets.size()) {
                            bucketIndex = 0;
                        }
                    }

                    auto& clearBucket = Buckets[firstBucketIndex];
                    clearBucket.Value = NUdf::TUnboxedValue();
                    clearBucket.HasValue = false;

                    if (aggregated) {
                        Self->State->SetValue(Ctx, NUdf::TUnboxedValue(*aggregated));
                        Self->Time->SetValue(Ctx, NUdf::TUnboxedValuePod((HopIndex - DelayHopCount) * HopTime));
                        Ready.emplace_back(Self->OutFinish->GetValue(Ctx));
                    }

                    ++newHops;
                    ++HopIndex;
                }

                if (hopIndex + DelayHopCount + 1 >= HopIndex) {
                    auto& bucket = Buckets[hopIndex % Buckets.size()];
                    if (!bucket.HasValue) {
                        bucket.Value = Self->OutInit->GetValue(Ctx);
                        bucket.HasValue = true;
                    } else {
                        Self->State->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        bucket.Value = Self->OutUpdate->GetValue(Ctx);
                    }
                } else {
                    ++thrownEvents;
                }
            }
        }


        const NUdf::TUnboxedValue Stream;
        const TSelf *const Self;

        const ui64 HopTime;
        const ui64 IntervalHopCount;
        const ui64 DelayHopCount;

        struct TBucket {
            NUdf::TUnboxedValue Value;
            bool HasValue = false;
        };

        std::vector<TBucket> Buckets; // circular buffer
        std::deque<NUdf::TUnboxedValue> Ready; // buffer for fetching results
        ui64 HopIndex = 0;
        bool Started = false;
        bool Finished = false;

        TComputationContext& Ctx;
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
        , Stream(stream)
        , Item(item)
        , State(state)
        , State2(state2)
        , Time(time)
        , InSave(inSave)
        , InLoad(inLoad)
        , OutTime(outTime)
        , OutInit(outInit)
        , OutUpdate(outUpdate)
        , OutSave(outSave)
        , OutLoad(outLoad)
        , OutMerge(outMerge)
        , OutFinish(outFinish)
        , Hop(hop)
        , Interval(interval)
        , Delay(delay)
        , StateType(stateType)
        , Packer(mutables)
    {
        Stateless = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto hopTime = Hop->GetValue(ctx).Get<i64>();
        const auto interval = Interval->GetValue(ctx).Get<i64>();
        const auto delay = Delay->GetValue(ctx).Get<i64>();

        // TODO: move checks from here
        MKQL_ENSURE(hopTime > 0, "hop must be positive");
        MKQL_ENSURE(interval >= hopTime, "interval should be greater or equal to hop");
        MKQL_ENSURE(delay >= hopTime, "delay should be greater or equal to hop");

        const auto intervalHopCount = interval / hopTime;
        const auto delayHopCount = delay / hopTime;

        MKQL_ENSURE(intervalHopCount <= 100000, "too many hops in interval");
        MKQL_ENSURE(delayHopCount <= 100000, "too many hops in delay");

        return ctx.HolderFactory.Create<TStreamValue>(Stream->GetValue(ctx), this, (ui64)hopTime, (ui64)intervalHopCount, (ui64)delayHopCount, ctx);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
        Own(Item);
        Own(State);
        Own(State2);
        Own(Time);
        Own(InSave);
        Own(InLoad);
        DependsOn(OutTime);
        DependsOn(OutInit);
        DependsOn(OutUpdate);
        DependsOn(OutSave);
        DependsOn(OutLoad);
        DependsOn(OutMerge);
        DependsOn(OutFinish);
        DependsOn(Hop);
        DependsOn(Interval);
        DependsOn(Delay);
    }

    IComputationNode* const Stream;

    IComputationExternalNode* const Item;
    IComputationExternalNode* const State;
    IComputationExternalNode* const State2;
    IComputationExternalNode* const Time;
    IComputationExternalNode* const InSave;
    IComputationExternalNode* const InLoad;

    IComputationNode* const OutTime;
    IComputationNode* const OutInit;
    IComputationNode* const OutUpdate;
    IComputationNode* const OutSave;
    IComputationNode* const OutLoad;
    IComputationNode* const OutMerge;
    IComputationNode* const OutFinish;

    IComputationNode* const Hop;
    IComputationNode* const Interval;
    IComputationNode* const Delay;

    TType* const StateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer;
};

}

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

}
}
