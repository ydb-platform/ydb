#include "mkql_multihopping.h"
#include "mkql_saveload.h"

#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/watermark_tracker.h>

#include <util/generic/scope.h>
#include <util/generic/ymath.h>
#include <util/generic/is_in.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TStatKey Hop_NewHopsCount("MultiHop_NewHopsCount", true);
const TStatKey Hop_FutureEventsCount("MultiHop_FarFutureEventsCount", true);
const TStatKey Hop_InvalidEventsCount("MultiHop_InvalidEventsCount", true);
const TStatKey Hop_LateThrownEventsCount("MultiHop_LateThrownEventsCount", true);
const TStatKey Hop_EmptyTimeCount("MultiHop_EmptyTimeCount", true);
const TStatKey Hop_KeysCount("MultiHop_KeysCount", true);
const TStatKey Hop_FarFutureStateSize("MultiHop_FarFutureStateSize", false);

constexpr ui32 StateVersion = 1;
constexpr ui32 StateVersionWithFutureEvents = 2;
using EPolicy = NYql::NHoppingWindow::EPolicy;

using TEqualsFunc = std::function<bool(NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod)>;
using THashFunc = std::function<NYql::NUdf::THashType(NUdf::TUnboxedValuePod)>;

class TMultiHoppingCoreWrapper: public TStatefulSourceComputationNode<TMultiHoppingCoreWrapper, true> {
    using TBaseComputation = TStatefulSourceComputationNode<TMultiHoppingCoreWrapper, true>;

public:
    using TSelf = TMultiHoppingCoreWrapper;

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
            bool dataWatermarks,
            bool watermarkMode,
            ui64 farFutureSizeLimit,
            ui64 farFutureTimeLimit,
            EPolicy earlyPolicy,
            EPolicy latePolicy,
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
            , FarFutureSizeLimit(farFutureSizeLimit)
            , FarFutureTimeLimit(Max(farFutureTimeLimit, intervalHopCount + delayHopCount))
            , EarlyPolicy(earlyPolicy)
            , LatePolicy(latePolicy)
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
            // Requires: Buckets.empty() || Buckets.size() >= IntervalHopCount size
            ui64 HopIndex;     // Start index of current window
            ui64 NextHopIndex; // Index after last defined event in the circular buffer (indexes *before* or equal to HopIndex are also valid and designates empty buffer)
            // Requires: NextHopIndex <= HopIndex + Buckets.size() [using infinite-precision]
            TMKQLMap<ui64, NUdf::TUnboxedValue> FutureEvents; // Aggregators for events >= HopIndex + Buckets.size()

            TKeyState(ui64 bucketsCount, ui64 hopIndex)
                : Buckets(bucketsCount)
                , HopIndex(hopIndex)
                , NextHopIndex(hopIndex)
            {
            }

            TKeyState(TKeyState&& state)
                : Buckets(std::move(state.Buckets))
                , HopIndex(state.HopIndex)
                , NextHopIndex(state.NextHopIndex)
                , FutureEvents(std::move(state.FutureEvents))
            {
            }
        };

        ui32 GetTraverseCount() const override {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
            Y_UNUSED(index);
            return Stream;
        }

        inline void SerializeState(TOutputSerializer& out, const NUdf::TUnboxedValue& value) const {
            Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(value));
            if (Self->StateType) {
                out.WriteUnboxedValue(Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType),
                                      Self->OutSave->GetValue(Ctx));
            }
        }

        NUdf::TUnboxedValue Save() const override {
            MKQL_ENSURE(Ready.empty(), "Inconsistent state to save, not all elements are fetched");
            bool hasFutureEvents = false;
            for (const auto& [key, state] : StatesMap) {
                if (!state.FutureEvents.empty()) {
                    hasFutureEvents = true;
                    break;
                }
            }
            // when no FutureEvents present, saves backward-compatible version 1 state;
            // when FutureEvents present, saves incompatible version 2 state;
            // acceptable since FutureEvents are only present in not-yet-released watermark code
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, (hasFutureEvents ? StateVersionWithFutureEvents : StateVersion), Ctx);

            out.Write<ui32>(StatesMap.size());
            for (const auto& [key, state] : StatesMap) {
                out.WriteUnboxedValue(Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), key);
                out(state.HopIndex);
                out.Write<ui32>(state.Buckets.size());
                for (const auto& bucket : state.Buckets) {
                    out(bucket.HasValue);
                    if (bucket.HasValue) {
                        SerializeState(out, bucket.Value);
                    }
                }
                if (!hasFutureEvents) {
                    continue;
                }
                out.Write<ui32>(state.FutureEvents.size());
                for (const auto& [time, value] : state.FutureEvents) {
                    out.Write<ui64>(time);
                    SerializeState(out, value);
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

        inline NUdf::TUnboxedValue DeserializeState(TInputSerializer& in) {
            if (Self->StateType) {
                Self->InLoad->SetValue(Ctx, in.ReadUnboxedValue(Self->StatePacker.RefMutableObject(Ctx, false, Self->StateType), Ctx));
            }
            return Self->OutLoad->GetValue(Ctx);
        }

        void LoadStateImpl(TInputSerializer& in) {
            const auto loadStateVersion = in.GetStateVersion();
            bool hasFutureEvents = false;
            if (loadStateVersion == StateVersionWithFutureEvents) {
                hasFutureEvents = true;
            } else if (loadStateVersion != StateVersion) {
                THROW yexception() << "Invalid state version " << loadStateVersion;
            }

            const auto statesMapSize = in.Read<ui32>();
            ClearState();
            StatesMap.reserve(statesMapSize);
            for (auto i = 0U; i < statesMapSize; ++i) {
                auto key = in.ReadUnboxedValue(Self->KeyPacker.RefMutableObject(Ctx, false, Self->KeyType), Ctx);
                const auto hopIndex = in.Read<ui64>();
                const auto bucketsSize = in.Read<ui32>();

                const auto hopBucketIndex = hopIndex % bucketsSize;

                TKeyState keyState(bucketsSize, hopIndex);
                for (ui64 i = 0; i < bucketsSize; ++i) {
                    auto& bucket = keyState.Buckets[i];
                    in(bucket.HasValue);
                    if (bucket.HasValue) {
                        const ui64 time = hopIndex + i + (i < hopBucketIndex ? bucketsSize : 0) - hopBucketIndex;
                        if (Y_UNLIKELY(time < hopIndex)) {
                            THROW yexception() << "Invalid state: time underflow " << time << " < " << hopIndex;
                        }
                        if (Y_UNLIKELY(time == Max<ui64>())) {
                            THROW yexception() << "Invalid state: invalid time " << time;
                        }
                        keyState.NextHopIndex = Max(keyState.NextHopIndex, time + 1);
                        bucket.Value = DeserializeState(in);
                    }
                }
                if (hasFutureEvents) {
                    const auto futureEventsSize = in.Read<ui32>();
                    for (ui32 i = 0; i < futureEventsSize; ++i) {
                        const auto time = in.Read<ui64>();
                        if (Y_UNLIKELY(Max(time, keyState.HopIndex) - keyState.HopIndex < keyState.Buckets.size())) {
                            THROW yexception() << "Invalid state: time underflow " << time << " < " << keyState.HopIndex << " + " << keyState.Buckets.size();
                        }
                        if (Y_UNLIKELY(time == Max<ui64>())) {
                            THROW yexception() << "Invalid state: invalid time " << time;
                        }
                        auto [_, inserted] = keyState.FutureEvents.emplace(time, DeserializeState(in));
                        Y_DEBUG_ABORT_UNLESS(inserted);
                        if (Y_UNLIKELY(!inserted)) {
                            THROW yexception() << "Invalid state: duplicated time " << time;
                        }
                    }
                    MKQL_ADD_STAT(Ctx.Stats, Hop_FarFutureStateSize, (i64)keyState.FutureEvents.size());
                }
                StatesMap.emplace(key, std::move(keyState));

                key.Ref();
            }
            MKQL_SET_STAT(Ctx.Stats, Hop_KeysCount, StatesMap.size());

            in(Finished);
        }

        bool HasListItems() const override {
            return false;
        }

        TMaybe<TInstant> GetWatermark() {
            return Watermark.WatermarkIn;
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Ready.empty()) { // Fastpath
                result = std::move(Ready.front());
                Ready.pop_front();
                return NUdf::EFetchStatus::Ok;
            }
            i64 farFutureEventsCount = 0;
            i64 invalidEventsThrown = 0;
            i64 lateEventsThrown = 0;
            i64 newHopsStat = 0;
            i64 emptyTimeCtStat = 0;
            i64 farFutureStateSizeChange = 0;

            Y_DEFER {
                MKQL_ADD_STAT(Ctx.Stats, Hop_FutureEventsCount, farFutureEventsCount);
                MKQL_ADD_STAT(Ctx.Stats, Hop_InvalidEventsCount, invalidEventsThrown);
                MKQL_ADD_STAT(Ctx.Stats, Hop_LateThrownEventsCount, lateEventsThrown);
                MKQL_ADD_STAT(Ctx.Stats, Hop_NewHopsCount, newHopsStat);
                MKQL_ADD_STAT(Ctx.Stats, Hop_EmptyTimeCount, emptyTimeCtStat);
                MKQL_ADD_STAT(Ctx.Stats, Hop_FarFutureStateSize, farFutureStateSizeChange);
            };

            for (NUdf::TUnboxedValue item;;) {
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

                const auto status = Stream.Fetch(item);
                if (status != NUdf::EFetchStatus::Ok) {
                    if (status == NUdf::EFetchStatus::Finish) {
                        CloseOldBuckets(Max<ui64>(), newHopsStat, farFutureStateSizeChange);
                        Finished = true;
                        continue;
                    } else if (status == NUdf::EFetchStatus::Yield) {
                        if (WatermarkMode) {
                            if (auto watermark = GetWatermark()) {
                                CloseOldBuckets(watermark->MicroSeconds(), newHopsStat, farFutureStateSizeChange);
                                PendingYield = true;
                                continue;
                            }
                        }
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
                auto hopIndex = ts / HopTime;

                const auto initialBufferPosition = WatermarkMode ? GetWatermark().GetOrElse(TInstant::Zero()).MicroSeconds() / HopTime : hopIndex;
                auto& keyState = GetOrCreateKeyState(key, initialBufferPosition);
                if (hopIndex < keyState.HopIndex) {
                    ++lateEventsThrown;
                    switch (LatePolicy) {
                        case EPolicy::Close:
                            Y_DEBUG_ABORT();
                            [[fallthrough]];
                        case EPolicy::Drop:
                            continue;
                        case EPolicy::Adjust:
                            hopIndex = keyState.HopIndex;
                            break;
                    }
                }
                if (Y_UNLIKELY(hopIndex == Max<ui64>())) { // reject invalid timestamp
                    ++invalidEventsThrown;
                    switch (EarlyPolicy) {
                        case EPolicy::Close:
                            [[fallthrough]];
                        case EPolicy::Drop:
                            continue;
                        case EPolicy::Adjust:
                            hopIndex = Max<ui64>() - 1;
                            break;
                    }
                }
                if (WatermarkMode && (hopIndex - keyState.HopIndex >= keyState.Buckets.size())) {
                    if (Y_UNLIKELY(hopIndex - keyState.HopIndex >= FarFutureTimeLimit) && keyState.HopIndex) {
                        switch (EarlyPolicy) {
                            case EPolicy::Drop:
                                continue;
                            case EPolicy::Adjust:
                                hopIndex = keyState.HopIndex + FarFutureTimeLimit - 1;
                                break;
                            case EPolicy::Close: {
                                auto closeBeforeIndex = Max<i64>(hopIndex + 1 - FarFutureTimeLimit, 0);
                                CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat, farFutureStateSizeChange);
                                break;
                            }
                        }
                    }
                    if (Y_LIKELY(hopIndex - keyState.HopIndex >= keyState.Buckets.size())) {
                        ++farFutureEventsCount;
                        auto it = keyState.FutureEvents.find(hopIndex);
                        if (it == keyState.FutureEvents.end()) {
                            keyState.FutureEvents.emplace(hopIndex, Self->OutInit->GetValue(Ctx));
                            ++farFutureStateSizeChange;

                            if (keyState.FutureEvents.size() > FarFutureSizeLimit) {
                                switch (EarlyPolicy) {
                                    case EPolicy::Close: {
                                        // move window so that first hop of FutureEvents became last hop of circular buffer
                                        auto first = keyState.FutureEvents.begin();
                                        auto closeBeforeIndex = first->first + 1 - keyState.Buckets.size();
                                        CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat, farFutureStateSizeChange);
                                        break;
                                    }
                                    case EPolicy::Adjust:
                                        Y_DEBUG_ABORT();
                                        [[fallthrough]];
                                    case EPolicy::Drop: {
                                        // drop last hop in FutureEvents
                                        auto last = keyState.FutureEvents.end();
                                        Y_DEBUG_ABORT_UNLESS(last != keyState.FutureEvents.begin());
                                        --last;
                                        keyState.FutureEvents.erase(last);
                                        --farFutureStateSizeChange;
                                        break;
                                    }
                                }
                                Y_DEBUG_ABORT_UNLESS(keyState.FutureEvents.size() == FarFutureSizeLimit);
                            }
                        } else {
                            auto& value = it->second;
                            Self->Key->SetValue(Ctx, std::move(key));
                            Self->State->SetValue(Ctx, std::move(value));
                            value = Self->OutUpdate->GetValue(Ctx);
                        }
                        continue;
                    }
                }

                // Overflow is not possible, because hopIndex is a product of a division
                if (!WatermarkMode) {
                    auto closeBeforeIndex = Max<i64>(hopIndex + 1 - DelayHopCount - IntervalHopCount, 0);
                    CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat, farFutureStateSizeChange);
                }

                auto& bucket = keyState.Buckets[hopIndex % keyState.Buckets.size()];
                if (!bucket.HasValue) {
                    bucket.Value = Self->OutInit->GetValue(Ctx);
                    bucket.HasValue = true;
                } else {
                    Self->Key->SetValue(Ctx, std::move(key));
                    Self->State->SetValue(Ctx, std::move(bucket.Value));
                    bucket.Value = Self->OutUpdate->GetValue(Ctx);
                }
                keyState.NextHopIndex = Max(keyState.NextHopIndex, hopIndex + 1);

                if (DataWatermarkTracker) {
                    if (const auto newWatermark = DataWatermarkTracker->HandleNextEventTime(ts)) {
                        CloseOldBuckets(*newWatermark, newHopsStat, farFutureStateSizeChange);
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
                keyHopIndex);
            if (iter.second) {
                key.Ref();
            }
            return iter.first->second;
        }

        inline void UpdateAggregation(const NUdf::TUnboxedValue& value, TMaybe<NUdf::TUnboxedValue>& aggregated) {
            if (!aggregated) { // todo: clone
                Self->InSave->SetValue(Ctx, NUdf::TUnboxedValue(value));
                Self->InLoad->SetValue(Ctx, Self->OutSave->GetValue(Ctx));
                aggregated = Self->OutLoad->GetValue(Ctx);
            } else {
                Self->State->SetValue(Ctx, NUdf::TUnboxedValue(value));
                Self->State2->SetValue(Ctx, std::move(*aggregated));
                aggregated = Self->OutMerge->GetValue(Ctx);
            }
        }

        inline ui64 FinishAggregation(const NUdf::TUnboxedValue& key, ui64 curHopIndex, TMaybe<NUdf::TUnboxedValue>& aggregated) {
            if (!aggregated) {
                return 0;
            }
            Self->Key->SetValue(Ctx, NUdf::TUnboxedValue(key));
            Self->State->SetValue(Ctx, std::move(*aggregated));
            // Outer code requires window end time (not start as could be expected)
            Self->Time->SetValue(Ctx, NUdf::TUnboxedValuePod((curHopIndex + IntervalHopCount) * HopTime));
            Ready.emplace_back(Self->OutFinish->GetValue(Ctx));
            return 1;
        }

        // Will return true if key state became empty
        bool CloseOldBucketsForKey(
            const NUdf::TUnboxedValue& key,
            TKeyState& keyState,
            const ui64 closeBeforeIndex, // Excluded bound
            i64& newHopsStat,
            i64& farFutureStateSizeChange)
        {
            auto& bucketsForKey = keyState.Buckets;
            auto curHopIndex = keyState.HopIndex;
            auto curHopIndexModBuckets = curHopIndex % bucketsForKey.size();

            if (curHopIndex > closeBeforeIndex) {
                return keyState.NextHopIndex <= keyState.HopIndex && keyState.FutureEvents.empty();
            }

            auto futureIt = keyState.FutureEvents.begin();

            Y_DEBUG_ABORT_UNLESS(keyState.FutureEvents.empty() || futureIt->first >= keyState.NextHopIndex);

            // be careful with overflows: HopIndex + Buckets.size() *may* overflow;
            // and NextHopIndex - HopIndex may underflow
            // (the only illegal value for time is Max<ui64>(), hence NextHopIndex never overflows to 0)
            const auto circularBufferLimit = Min(closeBeforeIndex, keyState.NextHopIndex);

            while (curHopIndex < circularBufferLimit) {
                TMaybe<NUdf::TUnboxedValue> aggregated;
                Y_DEBUG_ABORT_UNLESS(curHopIndexModBuckets == curHopIndex % bucketsForKey.size());
                // no overflow
                const ui64 intervalHopLimit = Min(IntervalHopCount, keyState.NextHopIndex - curHopIndex);
                auto jBucketIndex = curHopIndexModBuckets;
                for (ui64 j = 0; j < intervalHopLimit; ++j, ++jBucketIndex) {
                    if (jBucketIndex == bucketsForKey.size()) { // (from previous iteration)
                        jBucketIndex = 0;
                    }
                    Y_DEBUG_ABORT_UNLESS(jBucketIndex == (j + curHopIndex) % bucketsForKey.size());
                    const auto& bucket = bucketsForKey[jBucketIndex];
                    if (!bucket.HasValue) {
                        continue;
                    }
                    UpdateAggregation(bucket.Value, aggregated);
                }

                for (auto j = futureIt; j != keyState.FutureEvents.end() && j->first - IntervalHopCount < curHopIndex; ++j) {
                    // note: FutureEvents never overlaps with circular buffer
                    Y_DEBUG_ABORT_UNLESS(j->first >= curHopIndex + intervalHopLimit);
                    UpdateAggregation(j->second, aggregated);
                }

                newHopsStat += FinishAggregation(key, curHopIndex, aggregated);

                // advance circular buffer; curHopIndex % Buckets.size() becomes curHopIndex + Buckets.size()
                auto& clearBucket = bucketsForKey[curHopIndexModBuckets];
                clearBucket.Value = NUdf::TUnboxedValue();
                clearBucket.HasValue = false;

                ++curHopIndex;
                if (++curHopIndexModBuckets == bucketsForKey.size()) {
                    curHopIndexModBuckets = 0;
                }
            }

            if (keyState.FutureEvents.empty()) {
                curHopIndex = closeBeforeIndex;
            }

            Y_DEBUG_ABORT_UNLESS(futureIt == keyState.FutureEvents.end() || futureIt->first >= keyState.Buckets.size());
            // handle events from FutureEvents between end of circular buffer and closeBeforeIndex
            // (note that this loop won't be entered unless circular buffer is completely empty)
            for (; curHopIndex < closeBeforeIndex; ++curHopIndex) {
                Y_DEBUG_ABORT_UNLESS(curHopIndex >= keyState.NextHopIndex);
                // Skip completely empty windows: move curHopIndex
                // so that [curHopIndex:curHopIndex + IntervalHopCount] contains at least one key
                // Note: overflow impossible: futureIt->first >= Buckets.size() > IntervalHopHount - 1
                if (curHopIndex < futureIt->first - (IntervalHopCount - 1)) {
                    curHopIndex = futureIt->first - (IntervalHopCount - 1);
                    if (curHopIndex >= closeBeforeIndex) {
                        break;
                    }
                }

                TMaybe<NUdf::TUnboxedValue> aggregated;
                // Note: overflow impossible:
                // j->first >= futureIt->first since FutureEvents is ordered map
                // futureIt->first >= Buckets.size() >= IntervalHopCount
                for (auto j = futureIt; j != keyState.FutureEvents.end() && j->first - IntervalHopCount < curHopIndex; ++j) {
                    UpdateAggregation(j->second, aggregated);
                }

                newHopsStat += FinishAggregation(key, curHopIndex, aggregated);
                if (futureIt->first == curHopIndex) {
                    futureIt = keyState.FutureEvents.erase(futureIt);
                    --farFutureStateSizeChange;
                    if (futureIt == keyState.FutureEvents.end()) {
                        break;
                    }
                }
            }

            // move buckets from FutureEvents to circular buffer
            Y_DEBUG_ABORT_UNLESS(futureIt == keyState.FutureEvents.end() || futureIt->first >= keyState.Buckets.size());
            // overflow impossible (but closeBeforeIndex + keyState.Buckets.size()) *may* overflow)
            for (; futureIt != keyState.FutureEvents.end() && futureIt->first - keyState.Buckets.size() < closeBeforeIndex; futureIt = keyState.FutureEvents.erase(futureIt)) {
                auto& bucket = keyState.Buckets[futureIt->first % keyState.Buckets.size()];
                keyState.NextHopIndex = futureIt->first + 1;
                bucket.Value = std::move(futureIt->second);
                bucket.HasValue = true;
                --farFutureStateSizeChange;
            }

            keyState.HopIndex = closeBeforeIndex;
            return keyState.NextHopIndex <= keyState.HopIndex && keyState.FutureEvents.empty();
        }

        void CloseOldBuckets(ui64 watermarkTs, i64& newHops, i64& farFutureStateSizeChange) {
            const auto watermarkIndex = watermarkTs / HopTime;
            EraseNodesIf(StatesMap, [&](auto& iter) {
                auto& [key, val] = iter;
                ui64 closeBeforeIndex = Max<i64>(watermarkIndex + 1 - IntervalHopCount, 0);
                const auto keyStateBecameEmpty = CloseOldBucketsForKey(key, val, closeBeforeIndex, newHops, farFutureStateSizeChange);
                if (keyStateBecameEmpty) {
                    key.UnRef();
                }
                return keyStateBecameEmpty;
            });
            return;
        }

        void ClearState() {
            EraseNodesIf(StatesMap, [&](auto& iter) {
                MKQL_ADD_STAT(Ctx.Stats, Hop_FarFutureStateSize, -(i64)iter.second.FutureEvents.size());
                iter.first.UnRef();
                return true;
            });
            StatesMap.rehash(0);
        }

        const NUdf::TUnboxedValue Stream;
        const TSelf* const Self;

        const ui64 HopTime;
        const ui64 IntervalHopCount;
        const ui64 DelayHopCount;
        TWatermark& Watermark;
        bool WatermarkMode;
        ui64 FarFutureSizeLimit;
        ui64 FarFutureTimeLimit;
        EPolicy EarlyPolicy;
        EPolicy LatePolicy;
        bool PendingYield = false;

        using TStatesMap = std::unordered_map<
            NUdf::TUnboxedValuePod, TKeyState,
            THashFunc, TEqualsFunc,
            TMKQLAllocator<std::pair<const NUdf::TUnboxedValuePod, TKeyState>>>;

        TStatesMap StatesMap;                  // Map of states for each key
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
        IComputationNode* farFutureSizeLimit,
        IComputationNode* farFutureTimeLimitUs,
        IComputationNode* earlyPolicy,
        IComputationNode* latePolicy,
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
        , FarFutureSizeLimit(farFutureSizeLimit)
        , FarFutureTimeLimitUs(farFutureTimeLimitUs)
        , EarlyPolicy(earlyPolicy)
        , LatePolicy(latePolicy)
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
        const auto hopTime = Hop->GetValue(ctx).Get<ui64>();
        const auto interval = Interval->GetValue(ctx).Get<ui64>();
        const auto delay = Delay->GetValue(ctx).Get<ui64>();
        const auto dataWatermarks = DataWatermarks->GetValue(ctx).Get<bool>();
        const auto watermarkMode = WatermarkMode->GetValue(ctx).Get<bool>();
#define INIT_OPTIONAL_ARGUMENT(var, Type, Member, Default) \
    const auto var = (Member ? Member->GetValue(ctx) : NUdf::TUnboxedValue()).GetOrDefault<Type>(static_cast<Type>(NYql::NHoppingWindow::TSettings{}.Default))
        INIT_OPTIONAL_ARGUMENT(farFutureSizeLimit, ui64, FarFutureSizeLimit, FarFutureSizeLimit);
        INIT_OPTIONAL_ARGUMENT(farFutureTimeLimitUs, ui64, FarFutureTimeLimitUs, FarFutureTimeLimit.MicroSeconds());
        INIT_OPTIONAL_ARGUMENT(earlyPolicy, ui32, EarlyPolicy, EarlyPolicy);
        INIT_OPTIONAL_ARGUMENT(latePolicy, ui32, LatePolicy, LatePolicy);
#undef INIT_OPTIONAL_ARGUMENT
        MKQL_ENSURE(IsIn({static_cast<ui32>(EPolicy::Drop), static_cast<ui32>(EPolicy::Adjust), static_cast<ui32>(EPolicy::Close)}, earlyPolicy),
                    "Unexpected earlyPolicy " << earlyPolicy);
        MKQL_ENSURE(IsIn({static_cast<ui32>(EPolicy::Drop), static_cast<ui32>(EPolicy::Adjust)}, latePolicy),
                    "Unexpected latePolicy " << latePolicy);
        MKQL_ENSURE(!(earlyPolicy == static_cast<ui32>(EPolicy::Adjust) && farFutureSizeLimit != Max<ui64>()),
                    "Combination of EarlyPolicy=adjust with SizeLimit is not implemented, please set HoppingWindow SizeLimit to 'max' or use different EarlyPolicy");
        const auto intervalHopCount = interval / hopTime;
        const auto delayHopCount = delay / hopTime;

        return ctx.HolderFactory.Create<TStreamValue>(
            Stream->GetValue(ctx),
            this,
            hopTime,
            intervalHopCount,
            delayHopCount,
            dataWatermarks,
            watermarkMode,
            farFutureSizeLimit,
            CeilDiv(farFutureTimeLimitUs, hopTime),
            static_cast<EPolicy>(earlyPolicy),
            static_cast<EPolicy>(latePolicy),
            ctx,
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
        DependsOn(FarFutureSizeLimit);
        DependsOn(FarFutureTimeLimitUs);
        DependsOn(EarlyPolicy);
        DependsOn(LatePolicy);
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
    IComputationNode* const FarFutureSizeLimit;
    IComputationNode* const FarFutureTimeLimitUs;
    IComputationNode* const EarlyPolicy;
    IComputationNode* const LatePolicy;

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

} // namespace

IComputationNode* WrapMultiHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx, TWatermark& watermark) {
    MKQL_ENSURE(callable.GetInputsCount() > 20, "Expected at least 21 args");

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

#define GET_OPTIONAL_NODE(idx) ((callable.GetInputsCount() > idx && !callable.GetInput(idx).GetStaticType()->IsVoid()) ? LocateNode(ctx.NodeLocator, callable, idx) : nullptr)
    IComputationNode* farFutureSizeLimit = GET_OPTIONAL_NODE(21);
    IComputationNode* farFutureTimeLimitUs = GET_OPTIONAL_NODE(22);
    IComputationNode* earlyPolicy = GET_OPTIONAL_NODE(23);
    IComputationNode* latePolicy = GET_OPTIONAL_NODE(24);
#undef GET_OPTIONAL_NODE

    return new TMultiHoppingCoreWrapper(ctx.Mutables,
                                        stream, item, key, state, state2, time, inSave, inLoad, keyExtract,
                                        outTime, outInit, outUpdate, outSave, outLoad, outMerge, outFinish,
                                        hop, interval, delay, dataWatermarks, watermarkMode,
                                        farFutureSizeLimit, farFutureTimeLimitUs, earlyPolicy, latePolicy,
                                        keyType, stateType, watermark);
}

} // namespace NMiniKQL
} // namespace NKikimr
