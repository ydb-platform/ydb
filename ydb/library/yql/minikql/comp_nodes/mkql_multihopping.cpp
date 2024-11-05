#include "mkql_multihopping.h"
#include "mkql_saveload.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/watermark_tracker.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TStatKey Hop_NewHopsCount("MultiHop_NewHopsCount", true);
const TStatKey Hop_EarlyThrownEventsCount("MultiHop_EarlyThrownEventsCount", true);
const TStatKey Hop_LateThrownEventsCount("MultiHop_LateThrownEventsCount", true);
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
            bool watermarkMode,
            TComputationContext& ctx,
            const THashFunc& hash,
            const TEqualsFunc& equal,
            TWatermark& watermark)
            : TBase(memInfo)
            , Stream(std::move(stream))
            , Self(self)
            , HopTime(hopTime)
            , IntervalHopCount(intervalHopCount)
            , DelayHopCount(delayHopCount)
            , Watermark(watermark)
            , WatermarkMode(watermarkMode)
            , StatesMap(0, hash, equal)
            , Ctx(ctx)
        {
            if (!watermarkMode && dataWatermarks) {
                DataWatermarkTracker.emplace(TWatermarkTracker(delayHopCount * hopTime, hopTime));
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
            ui64 HopIndex; // Start index of current window

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
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);

            out.Write<ui32>(StatesMap.size());
            for (const auto& [key, state] : StatesMap) {
                out.WriteUnboxedValue(Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), key);
                out(state.HopIndex);
                out.Write<ui32>(state.Buckets.size());
                for (const auto& bucket : state.Buckets) {
                    out(bucket.HasValue);
                    if (bucket.HasValue) {
                        Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        if (Self->StateType) {
                            out.WriteUnboxedValue(Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType),
                                          Self->OutSave->GetValue(Ctx));
                        }
                    }
                }
            }

            out(Finished);
            return out.MakeState();
        }

        void Load(const NUdf::TStringRef& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);
            LoadStateImpl(in);
        }

        bool Load2(const NUdf::TUnboxedValue& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);
            LoadStateImpl(in);
            return true;
        }

        void LoadStateImpl(TInputSerializer& in) {
            const auto loadStateVersion = in.GetStateVersion();
            if (loadStateVersion != StateVersion) {
                THROW yexception() << "Invalid state version " << loadStateVersion;
            }

            const auto statesMapSize = in.Read<ui32>();
            ClearState();
            StatesMap.reserve(statesMapSize);
            for (auto i = 0U; i < statesMapSize; ++i) {
                auto key = in.ReadUnboxedValue(Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), Ctx);
                const auto hopIndex = in.Read<ui64>();
                const auto bucketsSize = in.Read<ui32>();

                TKeyState keyState(bucketsSize, hopIndex);
                for (auto& bucket : keyState.Buckets) {
                    in(bucket.HasValue);
                    if (bucket.HasValue) {
                        if (Self->StateType) {
                            Self->InLoad->SetValue(Ctx, in.ReadUnboxedValue(Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType), Ctx));
                        }
                        bucket.Value = Self->OutLoad->GetValue(Ctx);
                    }
                }
                StatesMap.emplace(key, std::move(keyState));
                key.Ref();
            }

            in(Finished);
        }

        bool HasListItems() const override {
            return false;
        }

        TInstant GetWatermark() {
            return Watermark.WatermarkIn;
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Ready.empty()) {
                result = std::move(Ready.front());
                Ready.pop_front();
                return NUdf::EFetchStatus::Ok;
            }
            if (PendingYield) {
                PendingYield = false;
                return NUdf::EFetchStatus::Yield;
            }

            if (Finished) {
                return NUdf::EFetchStatus::Finish;
            }

            i64 EarlyEventsThrown = 0;
            i64 LateEventsThrown = 0;
            i64 newHopsStat = 0;
            i64 emptyTimeCtStat = 0;

            Y_DEFER {
                MKQL_ADD_STAT(Ctx.Stats, Hop_EarlyThrownEventsCount, EarlyEventsThrown);
                MKQL_ADD_STAT(Ctx.Stats, Hop_LateThrownEventsCount, LateEventsThrown);
                MKQL_ADD_STAT(Ctx.Stats, Hop_NewHopsCount, newHopsStat);
                MKQL_ADD_STAT(Ctx.Stats, Hop_EmptyTimeCount, emptyTimeCtStat);
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
                        CloseOldBuckets(Max<ui64>(), newHopsStat);
                        Finished = true;
                        if (!Ready.empty()) {
                            result = std::move(Ready.front());
                            Ready.pop_front();
                            return NUdf::EFetchStatus::Ok;
                        }
                    } else if (status == NUdf::EFetchStatus::Yield) {
                        if (!WatermarkMode) {
                            return status;
                        }
                        PendingYield = true;
                        CloseOldBuckets(GetWatermark().MicroSeconds(), newHopsStat);
                        if (!Ready.empty()) {
                            result = std::move(Ready.front());
                            Ready.pop_front();
                            return NUdf::EFetchStatus::Ok;
                        }
                        PendingYield = false;
                        return NUdf::EFetchStatus::Yield;
                    }
                    return status;
                }

                Self->Item->SetValue(Ctx, std::move(item));
                auto key = Self->KeyExtract->GetValue(Ctx);
                const auto& time = Self->OutTime->GetValue(Ctx);
                if (!time) {
                    ++emptyTimeCtStat;
                    continue;
                }

                const auto ts = time.Get<ui64>();
                const auto hopIndex = ts / HopTime;

                auto& keyState = GetOrCreateKeyState(key, WatermarkMode ? GetWatermark().MicroSeconds() / HopTime : hopIndex);
                if (hopIndex < keyState.HopIndex) {
                    ++LateEventsThrown;
                    continue;
                }
                if (WatermarkMode && (hopIndex >= keyState.HopIndex + DelayHopCount + IntervalHopCount)) {
                    ++EarlyEventsThrown;
                    continue;
                }

                // Overflow is not possible, because hopIndex is a product of a division
                if (!WatermarkMode) {
                    auto closeBeforeIndex = Max<i64>(hopIndex + 1 - DelayHopCount - IntervalHopCount, 0);
                    CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat);
                }

                auto& bucket = keyState.Buckets[hopIndex % keyState.Buckets.size()];
                if (!bucket.HasValue) {
                    bucket.Value = Self->OutInit->GetValue(Ctx);
                    bucket.HasValue = true;
                } else {
                    Self->Key->SetValue(Ctx, NUdf::TUnboxedValue(key));
                    Self->State->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                    bucket.Value = Self->OutUpdate->GetValue(Ctx);
                }

                if (DataWatermarkTracker) {
                    const auto newWatermark = DataWatermarkTracker->HandleNextEventTime(ts);
                    if (newWatermark && !WatermarkMode) {
                        CloseOldBuckets(*newWatermark, newHopsStat);
                    }
                }
                MKQL_SET_STAT(Ctx.Stats, Hop_KeysCount, StatesMap.size());
            }
        }

        TKeyState& GetOrCreateKeyState(NUdf::TUnboxedValue& key, ui64 hopIndex) {
            i64 keyHopIndex = Max<i64>(hopIndex + 1 - IntervalHopCount, 0);
            // For first element we shouldn't forget windows in the past
            // Overflow is not possible, because hopIndex is a product of a division
            const auto iter = StatesMap.try_emplace(
                key,
                IntervalHopCount + DelayHopCount,
                keyHopIndex
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
            const ui64 closeBeforeIndex, // Excluded bound
            i64& newHopsStat)
        {
            auto& bucketsForKey = keyState.Buckets;

            bool becameEmpty = false;
            for (auto i = 0U; i < bucketsForKey.size(); ++i) {
                const auto curHopIndex = keyState.HopIndex;
                if (curHopIndex >= closeBeforeIndex) {
                    break;
                }

                i64 lastIndexWithValue = -1;
                TMaybe<NUdf::TUnboxedValue> aggregated;
                for (ui64 j = 0; j < IntervalHopCount; j++) {
                    const auto curBucketIndex = (curHopIndex + j) % bucketsForKey.size();
                    const auto& bucket = bucketsForKey[curBucketIndex];
                    if (!bucket.HasValue) {
                        continue;
                    }

                    if (!aggregated) { // todo: clone
                        Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        Self->InLoad->SetValue(Ctx, Self->OutSave->GetValue(Ctx));
                        aggregated = Self->OutLoad->GetValue(Ctx);
                    } else {
                        Self->State->SetValue(Ctx, NUdf::TUnboxedValue(bucket.Value));
                        Self->State2->SetValue(Ctx, NUdf::TUnboxedValue(*aggregated));
                        aggregated = Self->OutMerge->GetValue(Ctx);
                    }

                    lastIndexWithValue = Max<i64>(lastIndexWithValue, j);
                }

                if (aggregated) {
                    Self->Key->SetValue(Ctx, NUdf::TUnboxedValue(key));
                    Self->State->SetValue(Ctx, NUdf::TUnboxedValue(*aggregated));
                    // Outer code requires window end time (not start as could be expected)
                    Self->Time->SetValue(Ctx, NUdf::TUnboxedValuePod((curHopIndex + IntervalHopCount) * HopTime));
                    Ready.emplace_back(Self->OutFinish->GetValue(Ctx));

                    newHopsStat++;
                }

                auto& clearBucket = bucketsForKey[curHopIndex % bucketsForKey.size()];
                clearBucket.Value = NUdf::TUnboxedValue();
                clearBucket.HasValue = false;

                keyState.HopIndex++;

                if (lastIndexWithValue == 0) {
                    // Check if there is extra data in delayed buckets
                    for (ui64 j = IntervalHopCount; j < bucketsForKey.size(); j++) {
                        const auto curBucketIndex = (curHopIndex + j) % bucketsForKey.size();
                        const auto& bucket = bucketsForKey[curBucketIndex];
                        if (bucket.HasValue) {
                            lastIndexWithValue = Max<i64>(lastIndexWithValue, j);
                        }
                    }

                    if (lastIndexWithValue == 0) {
                        becameEmpty = true;
                        break;
                    }
                }
            }

            keyState.HopIndex = Max<ui64>(keyState.HopIndex, closeBeforeIndex);
            return becameEmpty;
        }

        void CloseOldBuckets(ui64 watermarkTs, i64& newHops) {
            const auto watermarkIndex = watermarkTs / HopTime;
            EraseNodesIf(StatesMap, [&](auto& iter) {
                auto& [key, val] = iter;
                ui64 closeBeforeIndex = watermarkIndex + 1 - IntervalHopCount;
                const auto keyStateBecameEmpty = CloseOldBucketsForKey(key, val, closeBeforeIndex, newHops);
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
        TWatermark& Watermark;
        bool WatermarkMode;
        bool PendingYield = false;

        using TStatesMap = std::unordered_map<
            NUdf::TUnboxedValuePod, TKeyState,
            THashFunc, TEqualsFunc,
            TMKQLAllocator<std::pair<const NUdf::TUnboxedValuePod, TKeyState>>>;

        TStatesMap StatesMap; // Map of states for each key
        std::deque<NUdf::TUnboxedValue> Ready; // buffer for fetching results
        bool Finished = false;

        TComputationContext& Ctx;
        std::optional<TWatermarkTracker> DataWatermarkTracker;
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
        IComputationNode* watermarkMode,
        TType* keyType,
        TType* stateType,
        TWatermark& watermark)
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
        , WatermarkMode(watermarkMode)
        , KeyType(keyType)
        , StateType(stateType)
        , KeyPacker(mutables)
        , StatePacker(mutables)
        , KeyTypes()
        , IsTuple(false)
        , UseIHash(false)
        , Watermark(watermark)
    {
        Stateless = false;
        bool encoded;
        GetDictionaryKeyTypes(keyType, KeyTypes, IsTuple, encoded, UseIHash);
        Y_ABORT_UNLESS(!encoded, "TODO");
        Equate = UseIHash ? MakeEquateImpl(KeyType) : nullptr;
        Hash = UseIHash ? MakeHashImpl(KeyType) : nullptr;
    }

    NUdf::TUnboxedValuePod CreateStream(TComputationContext& ctx) const {
        const auto hopTime = Hop->GetValue(ctx).Get<i64>();
        const auto interval = Interval->GetValue(ctx).Get<i64>();
        const auto delay = Delay->GetValue(ctx).Get<i64>();
        const auto dataWatermarks = DataWatermarks->GetValue(ctx).Get<bool>();
        const auto watermarkMode = WatermarkMode->GetValue(ctx).Get<bool>();

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
                                                      dataWatermarks, watermarkMode, ctx,
                                                      TValueHasher(KeyTypes, IsTuple, Hash.Get()),
                                                      TValueEqual(KeyTypes, IsTuple, Equate.Get()),
                                                      Watermark);
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& compCtx) const override {
        NUdf::TUnboxedValue& valueRef = ValueRef(compCtx);
        if (valueRef.IsInvalid()) {
            // Create new.
            valueRef = CreateStream(compCtx);
        } else if (valueRef.HasValue()) {
            MKQL_ENSURE(valueRef.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = valueRef.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue stream = CreateStream(compCtx);
                stream.Load2(valueRef);
                valueRef = stream;
            }
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
        DependsOn(WatermarkMode);
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
    IComputationNode* const WatermarkMode;

    TType* const KeyType;
    TType* const StateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> KeyPacker;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StatePacker;

    TKeyTypes KeyTypes;
    bool IsTuple;
    bool UseIHash;
    TWatermark& Watermark;

    NUdf::IEquate::TPtr Equate;
    NUdf::IHash::TPtr Hash;
};

}

IComputationNode* WrapMultiHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx, TWatermark& watermark) {
    MKQL_ENSURE(callable.GetInputsCount() == 21, "Expected 21 args");

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
    auto watermarkMode = LocateNode(ctx.NodeLocator, callable, 20);

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
        hop, interval, delay, dataWatermarks, watermarkMode, keyType, stateType, watermark);
}

}
}
