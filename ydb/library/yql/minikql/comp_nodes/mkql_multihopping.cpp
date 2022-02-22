#include "mkql_multihopping.h"
#include "mkql_saveload.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/watermark_tracker.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TStatKey Hop_NewHopsCount("MultiHop_NewHopsCount", true);
const TStatKey Hop_ThrownEventsCount("MultiHop_ThrownEventsCount", true);
const TStatKey Hop_EmptyTimeCount("MultiHop_EmptyTimeCount", true);
const TStatKey Hop_KeysCount("MultiHop_KeysCount", true);

constexpr ui32 StateVersion = 1;

using TEqualsFunc = std::function<bool(NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod)>;
using THashFunc = std::function<NYql::NUdf::THashType(NUdf::TUnboxedValuePod)>;

class TMultiHoppingCoreWrapper : public TStatefulSourceComputationNode<TMultiHoppingCoreWrapper, true> {
    using TBaseComputation = TStatefulSourceComputationNode<TMultiHoppingCoreWrapper, true>;
public:
    using TSelf = TMultiHoppingCoreWrapper;

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
            bool dataWatermarks,
            TComputationContext& ctx,
            const THashFunc& hash,
            const TEqualsFunc& equal)
            : TBase(memInfo)
            , Stream(std::move(stream))
            , Self(self)
            , HopTime(hopTime)
            , IntervalHopCount(intervalHopCount)
            , DelayHopCount(delayHopCount)
            , StatesMap(0, hash, equal)
            , Ctx(ctx)
        {
            if (dataWatermarks) {
                WatermarkTracker.emplace(TWatermarkTracker(delayHopCount * hopTime, hopTime));
            }
        }

        ~TStreamValue() {
            ClearState();
        }

    private:
        struct TBucket {
            NUdf::TUnboxedValue Value;
            bool HasValue = false;
        };

        struct TKeyState {
            std::vector<TBucket, TMKQLAllocator<TBucket>> Buckets; // circular buffer
            ui64 HopIndex;

            TKeyState(ui64 bucketsCount, ui64 hopIndex)
                : Buckets(bucketsCount)
                , HopIndex(hopIndex)
            {}

            TKeyState(TKeyState&& state)
                : Buckets(std::move(state.Buckets))
                , HopIndex(state.HopIndex)
            {}
        };

        ui32 GetTraverseCount() const override {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::TUnboxedValue Save() const override {
            MKQL_ENSURE(Ready.empty(), "Inconsistent state to save, not all elements are fetched");

            TString out;
            WriteUi32(out, StateVersion);
            WriteUi32(out, StatesMap.size());
            for (const auto& [key, state] : StatesMap) {
                WriteUnboxedValue(out, Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), key);
                WriteUi64(out, state.HopIndex);
                WriteUi32(out, state.Buckets.size());
                for (const auto& bucket : state.Buckets) {
                    WriteBool(out, bucket.HasValue);
                    if (bucket.HasValue) {
                        Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        if (Self->StateType) {
                            WriteUnboxedValue(out, Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType),
                                          Self->OutSave->GetValue(Ctx));
                        }
                    }
                }
            }

            WriteBool(out, Finished);

            auto strRef = NUdf::TStringRef(out.data(), out.size());
            return MakeString(strRef);
        }

        void Load(const NUdf::TStringRef& state) override {
            TStringBuf in(state.Data(), state.Size());

            const auto stateVersion = ReadUi32(in);
            if (stateVersion == 1) {
                const auto statesMapSize = ReadUi32(in);
                ClearState();
                StatesMap.reserve(statesMapSize);
                for (int i = 0; i < statesMapSize; i++) {
                    auto key = ReadUnboxedValue(in, Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), Ctx);
                    const auto hopIndex = ReadUi64(in);
                    const auto bucketsSize = ReadUi32(in);

                    TKeyState keyState(bucketsSize, hopIndex);
                    for (auto& bucket : keyState.Buckets) {
                        bucket.HasValue = ReadBool(in);
                        if (bucket.HasValue) {
                            if (Self->StateType) {
                                Self->InLoad->SetValue(Ctx, ReadUnboxedValue(in, Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType), Ctx));
                            }
                            bucket.Value = Self->OutLoad->GetValue(Ctx);
                        }
                    }
                    StatesMap.emplace(key.Release(), std::move(keyState));
                }

                Finished = ReadBool(in);
            } else {
                THROW yexception() << "Invalid state version " << stateVersion;
            }
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
            i64 emptyTimeCt = 0;
            Y_DEFER {
                if (thrownEvents) {
                    MKQL_ADD_STAT(Ctx.Stats, Hop_ThrownEventsCount, thrownEvents);
                }
                if (newHops) {
                    MKQL_ADD_STAT(Ctx.Stats, Hop_NewHopsCount, newHops);
                }
                if (emptyTimeCt) {
                    MKQL_ADD_STAT(Ctx.Stats, Hop_EmptyTimeCount, emptyTimeCt);
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
                        CloseOldBuckets(Max<ui64>(), newHops);
                        Finished = true;
                        if (!Ready.empty()) {
                            result = std::move(Ready.front());
                            Ready.pop_front();
                            return NUdf::EFetchStatus::Ok;
                        }
                    }
                    return status;
                }

                Self->Item->SetValue(Ctx, std::move(item));
                auto key = Self->KeyExtract->GetValue(Ctx);
                const auto& time = Self->OutTime->GetValue(Ctx);
                if (!time) {
                    ++emptyTimeCt;
                    continue;
                }

                const auto ts = time.Get<ui64>();
                const auto hopIndex = ts / HopTime;
                auto& keyState = GetOrCreateKeyState(key, hopIndex + 1);

                CloseOldBucketsForKey(key, keyState, hopIndex, newHops);

                if (hopIndex + DelayHopCount + 1 >= keyState.HopIndex) {
                    auto& bucket = keyState.Buckets[hopIndex % keyState.Buckets.size()];
                    if (!bucket.HasValue) {
                        bucket.Value = Self->OutInit->GetValue(Ctx);
                        bucket.HasValue = true;
                    } else {
                        Self->Key->SetValue(Ctx, NUdf::TUnboxedValue(key));
                        Self->State->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        bucket.Value = Self->OutUpdate->GetValue(Ctx);
                    }
                } else {
                    ++thrownEvents;
                }

                if (WatermarkTracker) {
                    const auto newWatermark = WatermarkTracker->HandleNextEventTime(ts);
                    if (newWatermark) {
                        CloseOldBuckets(*newWatermark, newHops);
                    }
                }
                MKQL_SET_STAT(Ctx.Stats, Hop_KeysCount, StatesMap.size());
            }
        }

        TKeyState& GetOrCreateKeyState(NUdf::TUnboxedValue& key, ui64 hopIndex) {
            const auto iter = StatesMap.try_emplace(
                key,
                IntervalHopCount + DelayHopCount,
                hopIndex
            );
            if (iter.second) {
                key.Ref();
            }
            return iter.first->second;
        }

        // Will return true if key state became empty
        bool CloseOldBucketsForKey(
            const NUdf::TUnboxedValue& key,
            TKeyState& keyState,
            const ui64 hopIndex,
            i64& newHops)
        {
            auto& bucketsForKey = keyState.Buckets;
            const auto endIndex = Min(hopIndex, keyState.HopIndex + bucketsForKey.size()); // TODO: fix possible overflow

            for (auto& hopIndexForKey = keyState.HopIndex; hopIndexForKey <= endIndex; hopIndexForKey++) {
                auto firstBucketIndex = hopIndexForKey % bucketsForKey.size();

                auto bucketIndex = firstBucketIndex;
                TMaybe<NUdf::TUnboxedValue> aggregated;

                for (ui64 i = 0; i < IntervalHopCount; ++i) {
                    const auto& bucket = bucketsForKey[bucketIndex];
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
                    if (++bucketIndex == bucketsForKey.size()) {
                        bucketIndex = 0;
                    }
                }

                auto& clearBucket = bucketsForKey[firstBucketIndex];
                clearBucket.Value = NUdf::TUnboxedValue();
                clearBucket.HasValue = false;

                if (aggregated) {
                    Self->Key->SetValue(Ctx, NUdf::TUnboxedValue(key));
                    Self->State->SetValue(Ctx, NUdf::TUnboxedValue(*aggregated));
                    Self->Time->SetValue(Ctx, NUdf::TUnboxedValuePod((hopIndexForKey - DelayHopCount) * HopTime));
                    Ready.emplace_back(Self->OutFinish->GetValue(Ctx));
                }

                ++newHops;
            }

            return endIndex < hopIndex;
        }

        void CloseOldBuckets(ui64 watermarkTs, i64& newHops) {
            const auto watermarkIndex = watermarkTs / HopTime;
            EraseNodesIf(StatesMap, [&](auto& iter) {
                auto& [key, val] = iter;
                const auto keyStateBecameEmpty = CloseOldBucketsForKey(key, val, watermarkIndex, newHops);
                if (keyStateBecameEmpty) {
                    key.UnRef();
                }
                return keyStateBecameEmpty;
            });
            return;
        }

        void ClearState() {
            EraseNodesIf(StatesMap, [&](auto& iter) {
                iter.first.UnRef();
                return true;
            });
            StatesMap.rehash(0);
        }

        const NUdf::TUnboxedValue Stream;
        const TSelf *const Self;

        const ui64 HopTime;
        const ui64 IntervalHopCount;
        const ui64 DelayHopCount;

        using TStatesMap = std::unordered_map<
            NUdf::TUnboxedValuePod, TKeyState,
            THashFunc, TEqualsFunc,
            TMKQLAllocator<std::pair<const NUdf::TUnboxedValuePod, TKeyState>>>;

        TStatesMap StatesMap; // Map of states for each key
        std::deque<NUdf::TUnboxedValue> Ready; // buffer for fetching results
        bool Finished = false;

        TComputationContext& Ctx;
        std::optional<TWatermarkTracker> WatermarkTracker;
    };

    TMultiHoppingCoreWrapper(
        TComputationMutables& mutables,
        IComputationNode* stream,
        IComputationExternalNode* item,
        IComputationExternalNode* key,
        IComputationExternalNode* state,
        IComputationExternalNode* state2,
        IComputationExternalNode* time,
        IComputationExternalNode* inSave,
        IComputationExternalNode* inLoad,
        IComputationNode* keyExtract,
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
        IComputationNode* dataWatermarks,
        TType* keyType,
        TType* stateType)
        : TBaseComputation(mutables)
        , Stream(stream)
        , Item(item)
        , Key(key)
        , State(state)
        , State2(state2)
        , Time(time)
        , InSave(inSave)
        , InLoad(inLoad)
        , KeyExtract(keyExtract)
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
        , DataWatermarks(dataWatermarks)
        , KeyType(keyType)
        , StateType(stateType)
        , KeyPacker(mutables)
        , StatePacker(mutables)
        , KeyTypes()
        , IsTuple()
    {
        Stateless = false;

        bool encoded;
        GetDictionaryKeyTypes(keyType, KeyTypes, IsTuple, encoded);
        Y_VERIFY(!encoded, "TODO");
    }

    NUdf::TUnboxedValuePod CreateStream(TComputationContext& ctx) const {
        const auto hopTime = Hop->GetValue(ctx).Get<i64>();
        const auto interval = Interval->GetValue(ctx).Get<i64>();
        const auto delay = Delay->GetValue(ctx).Get<i64>();
        const auto dataWatermarks = DataWatermarks->GetValue(ctx).Get<bool>();

        // TODO: move checks from here
        MKQL_ENSURE(hopTime > 0, "hop must be positive");
        MKQL_ENSURE(interval >= hopTime, "interval should be greater or equal to hop");
        MKQL_ENSURE(delay >= hopTime, "delay should be greater or equal to hop");

        const auto intervalHopCount = interval / hopTime;
        const auto delayHopCount = delay / hopTime;

        MKQL_ENSURE(intervalHopCount <= 100000, "too many hops in interval");
        MKQL_ENSURE(delayHopCount <= 100000, "too many hops in delay");

        return ctx.HolderFactory.Create<TStreamValue>(Stream->GetValue(ctx), this, (ui64)hopTime,
                                                      (ui64)intervalHopCount, (ui64)delayHopCount,
                                                      dataWatermarks, ctx,
                                                      TValueHasher(KeyTypes, IsTuple),
                                                      TValueEqual(KeyTypes, IsTuple));
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        NUdf::TUnboxedValue& valueRef = ValueRef(compCtx);
        if (valueRef.IsInvalid()) {
            // Create new.
            valueRef = CreateStream(compCtx);
        } else if (valueRef.HasValue() && !valueRef.IsBoxed()) {
            // Load from saved state.
            NUdf::TUnboxedValue stream = CreateStream(compCtx);
            stream.Load(valueRef.AsStringRef());
            valueRef = stream;
        }

        return valueRef;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
        Own(Item);
        Own(Key);
        Own(State);
        Own(State2);
        Own(Time);
        Own(InSave);
        Own(InLoad);
        DependsOn(KeyExtract);
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
        DependsOn(DataWatermarks);
    }

    IComputationNode* const Stream;

    IComputationExternalNode* const Item;
    IComputationExternalNode* const Key;
    IComputationExternalNode* const State;
    IComputationExternalNode* const State2;
    IComputationExternalNode* const Time;
    IComputationExternalNode* const InSave;
    IComputationExternalNode* const InLoad;

    IComputationNode* const KeyExtract;
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
    IComputationNode* const DataWatermarks;

    TType* const KeyType;
    TType* const StateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> KeyPacker;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StatePacker;

    TKeyTypes KeyTypes;
    bool IsTuple;
};

}

IComputationNode* WrapMultiHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 20, "Expected 20 args");

    auto hasSaveLoad = !callable.GetInput(12).GetStaticType()->IsVoid();

    IComputationExternalNode* inSave = nullptr;
    IComputationNode* outSave = nullptr;
    IComputationExternalNode* inLoad = nullptr;
    IComputationNode* outLoad = nullptr;

    auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    const auto keyType = callable.GetInput(8).GetStaticType();

    auto stream = LocateNode(ctx.NodeLocator, callable, 0);

    auto keyExtract = LocateNode(ctx.NodeLocator, callable, 8);
    auto outTime = LocateNode(ctx.NodeLocator, callable, 9);
    auto outInit = LocateNode(ctx.NodeLocator, callable, 10);
    auto outUpdate = LocateNode(ctx.NodeLocator, callable, 11);
    if (hasSaveLoad) {
        outSave = LocateNode(ctx.NodeLocator, callable, 12);
        outLoad = LocateNode(ctx.NodeLocator, callable, 13);
    }
    auto outMerge = LocateNode(ctx.NodeLocator, callable, 14);
    auto outFinish = LocateNode(ctx.NodeLocator, callable, 15);

    auto hop = LocateNode(ctx.NodeLocator, callable, 16);
    auto interval = LocateNode(ctx.NodeLocator, callable, 17);
    auto delay = LocateNode(ctx.NodeLocator, callable, 18);
    auto dataWatermarks = LocateNode(ctx.NodeLocator, callable, 19);

    auto item = LocateExternalNode(ctx.NodeLocator, callable, 1);
    auto key = LocateExternalNode(ctx.NodeLocator, callable, 2);
    auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);
    auto state2 = LocateExternalNode(ctx.NodeLocator, callable, 4);
    auto time = LocateExternalNode(ctx.NodeLocator, callable, 5);
    if (hasSaveLoad) {
        inSave = LocateExternalNode(ctx.NodeLocator, callable, 6);
        inLoad = LocateExternalNode(ctx.NodeLocator, callable, 7);
    }

    auto stateType = hasSaveLoad ? callable.GetInput(12).GetStaticType() : nullptr;

    return new TMultiHoppingCoreWrapper(ctx.Mutables,
        stream, item, key, state, state2, time, inSave, inLoad, keyExtract,
        outTime, outInit, outUpdate, outSave, outLoad, outMerge, outFinish,
        hop, interval, delay, dataWatermarks, keyType, stateType);
}

}
}
