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

namespace NKikimr::NMiniKQL {

namespace {

const TStatKey Hop_NewHopsCount("MultiHop_NewHopsCount", /*deriv=*/true);
const TStatKey Hop_FutureEventsCount("MultiHop_FarFutureEventsCount", /*deriv=*/true);
const TStatKey Hop_InvalidEventsCount("MultiHop_InvalidEventsCount", /*deriv=*/true);
const TStatKey Hop_LateThrownEventsCount("MultiHop_LateThrownEventsCount", /*deriv=*/true);
const TStatKey Hop_EmptyTimeCount("MultiHop_EmptyTimeCount", /*deriv=*/true);
const TStatKey Hop_KeysCount("MultiHop_KeysCount", /*deriv=*/true);
const TStatKey Hop_FarFutureStateSize("MultiHop_FarFutureStateSize", /*deriv=*/false);

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
            , Stream_(std::move(stream))
            , Self_(self)
            , HopTime_(hopTime)
            , IntervalHopCount_(intervalHopCount)
            , DelayHopCount_(delayHopCount)
            , Watermark_(watermark)
            , WatermarkMode_(watermarkMode)
            , FarFutureSizeLimit_(farFutureSizeLimit)
            , FarFutureTimeLimit_(Max(farFutureTimeLimit, intervalHopCount + delayHopCount))
            , EarlyPolicy_(earlyPolicy)
            , LatePolicy_(latePolicy)
            , StatesMap_(0, hash, equal)
            , Ctx_(ctx)
        {
            if (!watermarkMode && dataWatermarks) {
                DataWatermarkTracker_.emplace(TWatermarkTracker(delayHopCount * hopTime, hopTime));
            }
        }

        ~TStreamValue() override {
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
            return Stream_;
        }

        inline void SerializeState(TOutputSerializer& out, const NUdf::TUnboxedValue& value) const {
            Self_->InSave_->SetValue(Ctx_, NUdf::TUnboxedValue(value));
            if (Self_->StateType_) {
                out.WriteUnboxedValue(Self_->StatePacker_.RefMutableObject(Ctx_, false, Self_->StateType_),
                                      Self_->OutSave_->GetValue(Ctx_));
            }
        }

        NUdf::TUnboxedValue Save() const override {
            MKQL_ENSURE(Ready_.empty(), "Inconsistent state to save, not all elements are fetched");
            bool hasFutureEvents = false;
            for (const auto& [key, state] : StatesMap_) {
                if (!state.FutureEvents.empty()) {
                    hasFutureEvents = true;
                    break;
                }
            }
            // when no FutureEvents present, saves backward-compatible version 1 state;
            // when FutureEvents present, saves incompatible version 2 state;
            // acceptable since FutureEvents are only present in not-yet-released watermark code
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, (hasFutureEvents ? StateVersionWithFutureEvents : StateVersion), Ctx_);

            out.Write<ui32>(StatesMap_.size());
            for (const auto& [key, state] : StatesMap_) {
                out.WriteUnboxedValue(Self_->KeyPacker_.RefMutableObject(Ctx_, false, Self_->KeyType_), key);
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

            out(Finished_);
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
            if (Self_->StateType_) {
                Self_->InLoad_->SetValue(Ctx_, in.ReadUnboxedValue(Self_->StatePacker_.RefMutableObject(Ctx_, false, Self_->StateType_), Ctx_));
            }
            return Self_->OutLoad_->GetValue(Ctx_);
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
            StatesMap_.reserve(statesMapSize);
            for (auto i = 0U; i < statesMapSize; ++i) {
                auto key = in.ReadUnboxedValue(Self_->KeyPacker_.RefMutableObject(Ctx_, false, Self_->KeyType_), Ctx_);
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
                    MKQL_ADD_STAT(Ctx_.Stats, Hop_FarFutureStateSize, (i64)keyState.FutureEvents.size());
                }
                StatesMap_.emplace(key, std::move(keyState));

                key.Ref();
            }
            MKQL_SET_STAT(Ctx_.Stats, Hop_KeysCount, StatesMap_.size());

            in(Finished_);
        }

        bool HasListItems() const override {
            return false;
        }

        TMaybe<TInstant> GetWatermark() {
            return Watermark_.WatermarkIn;
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Ready_.empty()) { // Fastpath
                result = std::move(Ready_.front());
                Ready_.pop_front();
                return NUdf::EFetchStatus::Ok;
            }
            i64 farFutureEventsCount = 0;
            i64 invalidEventsThrown = 0;
            i64 lateEventsThrown = 0;
            i64 newHopsStat = 0;
            i64 emptyTimeCtStat = 0;
            i64 farFutureStateSizeChange = 0;

            Y_DEFER {
                MKQL_ADD_STAT(Ctx_.Stats, Hop_FutureEventsCount, farFutureEventsCount);
                MKQL_ADD_STAT(Ctx_.Stats, Hop_InvalidEventsCount, invalidEventsThrown);
                MKQL_ADD_STAT(Ctx_.Stats, Hop_LateThrownEventsCount, lateEventsThrown);
                MKQL_ADD_STAT(Ctx_.Stats, Hop_NewHopsCount, newHopsStat);
                MKQL_ADD_STAT(Ctx_.Stats, Hop_EmptyTimeCount, emptyTimeCtStat);
                MKQL_ADD_STAT(Ctx_.Stats, Hop_FarFutureStateSize, farFutureStateSizeChange);
            };

            for (NUdf::TUnboxedValue item;;) {
                if (!Ready_.empty()) {
                    result = std::move(Ready_.front());
                    Ready_.pop_front();
                    return NUdf::EFetchStatus::Ok;
                }

                if (PendingYield_) {
                    PendingYield_ = false;
                    return NUdf::EFetchStatus::Yield;
                }

                if (Finished_) {
                    return NUdf::EFetchStatus::Finish;
                }

                const auto status = Stream_.Fetch(item);
                if (status != NUdf::EFetchStatus::Ok) {
                    if (status == NUdf::EFetchStatus::Finish) {
                        CloseOldBuckets(Max<ui64>(), newHopsStat, farFutureStateSizeChange);
                        Finished_ = true;
                        continue;
                    } else if (status == NUdf::EFetchStatus::Yield) {
                        if (WatermarkMode_) {
                            if (auto watermark = GetWatermark()) {
                                CloseOldBuckets(watermark->MicroSeconds(), newHopsStat, farFutureStateSizeChange);

                                // A `Yield` during active watermark is considered permanent.
                                // Until that `Yield` is produced into output stream, all subsequent runs must also yield.
                                // Therefore, input may be read and buckets may be closed only once.
                                //
                                // **note:** Unlike most MKQL nodes, this node may produce a `Yield` without receiving one from its input.
                                //           This exception is allowed here due to watermark propagation logic.
                                PendingYield_ = true;
                                continue;
                            }
                        }
                        return NUdf::EFetchStatus::Yield;
                    }
                    return status;
                }

                Self_->Item_->SetValue(Ctx_, std::move(item));
                auto key = Self_->KeyExtract_->GetValue(Ctx_);
                const auto& time = Self_->OutTime_->GetValue(Ctx_);
                if (!time) {
                    ++emptyTimeCtStat;
                    continue;
                }

                const auto ts = time.Get<ui64>();
                auto hopIndex = ts / HopTime_;

                const auto initialBufferPosition = WatermarkMode_ ? GetWatermark().GetOrElse(TInstant::Zero()).MicroSeconds() / HopTime_ : hopIndex;
                auto& keyState = GetOrCreateKeyState(key, initialBufferPosition);
                if (hopIndex < keyState.HopIndex) {
                    ++lateEventsThrown;
                    switch (LatePolicy_) {
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
                    switch (EarlyPolicy_) {
                        case EPolicy::Close:
                            [[fallthrough]];
                        case EPolicy::Drop:
                            continue;
                        case EPolicy::Adjust:
                            hopIndex = Max<ui64>() - 1;
                            break;
                    }
                }
                if (WatermarkMode_ && (hopIndex - keyState.HopIndex >= keyState.Buckets.size())) {
                    if (Y_UNLIKELY(hopIndex - keyState.HopIndex >= FarFutureTimeLimit_) && keyState.HopIndex) {
                        switch (EarlyPolicy_) {
                            case EPolicy::Drop:
                                continue;
                            case EPolicy::Adjust:
                                hopIndex = keyState.HopIndex + FarFutureTimeLimit_ - 1;
                                break;
                            case EPolicy::Close: {
                                auto closeBeforeIndex = Max<i64>(hopIndex + 1 - FarFutureTimeLimit_, 0);
                                CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat, farFutureStateSizeChange);
                                break;
                            }
                        }
                    }
                    if (Y_LIKELY(hopIndex - keyState.HopIndex >= keyState.Buckets.size())) {
                        ++farFutureEventsCount;
                        auto it = keyState.FutureEvents.find(hopIndex);
                        if (it == keyState.FutureEvents.end()) {
                            keyState.FutureEvents.emplace(hopIndex, Self_->OutInit_->GetValue(Ctx_));
                            ++farFutureStateSizeChange;

                            if (keyState.FutureEvents.size() > FarFutureSizeLimit_) {
                                switch (EarlyPolicy_) {
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
                                Y_DEBUG_ABORT_UNLESS(keyState.FutureEvents.size() == FarFutureSizeLimit_);
                            }
                        } else {
                            auto& value = it->second;
                            Self_->Key_->SetValue(Ctx_, std::move(key));
                            Self_->State_->SetValue(Ctx_, std::move(value));
                            value = Self_->OutUpdate_->GetValue(Ctx_);
                        }
                        continue;
                    }
                }

                // Overflow is not possible, because hopIndex is a product of a division
                if (!WatermarkMode_) {
                    auto closeBeforeIndex = Max<i64>(hopIndex + 1 - DelayHopCount_ - IntervalHopCount_, 0);
                    CloseOldBucketsForKey(key, keyState, closeBeforeIndex, newHopsStat, farFutureStateSizeChange);
                }

                auto& bucket = keyState.Buckets[hopIndex % keyState.Buckets.size()];
                if (!bucket.HasValue) {
                    bucket.Value = Self_->OutInit_->GetValue(Ctx_);
                    bucket.HasValue = true;
                } else {
                    Self_->Key_->SetValue(Ctx_, std::move(key));
                    Self_->State_->SetValue(Ctx_, std::move(bucket.Value));
                    bucket.Value = Self_->OutUpdate_->GetValue(Ctx_);
                }
                keyState.NextHopIndex = Max(keyState.NextHopIndex, hopIndex + 1);

                if (DataWatermarkTracker_) {
                    if (const auto newWatermark = DataWatermarkTracker_->HandleNextEventTime(ts)) {
                        CloseOldBuckets(*newWatermark, newHopsStat, farFutureStateSizeChange);
                    }
                }
                MKQL_SET_STAT(Ctx_.Stats, Hop_KeysCount, StatesMap_.size());
            }
        }

        TKeyState& GetOrCreateKeyState(NUdf::TUnboxedValue& key, ui64 hopIndex) {
            i64 keyHopIndex = Max<i64>(hopIndex + 1 - IntervalHopCount_, 0);
            // For first element we shouldn't forget windows in the past
            // Overflow is not possible, because hopIndex is a product of a division
            const auto iter = StatesMap_.try_emplace(
                key,
                IntervalHopCount_ + DelayHopCount_,
                keyHopIndex);
            if (iter.second) {
                key.Ref();
            }
            return iter.first->second;
        }

        inline void UpdateAggregation(const NUdf::TUnboxedValue& value, TMaybe<NUdf::TUnboxedValue>& aggregated) {
            if (!aggregated) { // todo: clone
                Self_->InSave_->SetValue(Ctx_, NUdf::TUnboxedValue(value));
                Self_->InLoad_->SetValue(Ctx_, Self_->OutSave_->GetValue(Ctx_));
                aggregated = Self_->OutLoad_->GetValue(Ctx_);
            } else {
                Self_->State_->SetValue(Ctx_, NUdf::TUnboxedValue(value));
                Self_->State2_->SetValue(Ctx_, std::move(*aggregated));
                aggregated = Self_->OutMerge_->GetValue(Ctx_);
            }
        }

        inline ui64 FinishAggregation(const NUdf::TUnboxedValue& key, ui64 curHopIndex, TMaybe<NUdf::TUnboxedValue>& aggregated) {
            if (!aggregated) {
                return 0;
            }
            Self_->Key_->SetValue(Ctx_, NUdf::TUnboxedValue(key));
            Self_->State_->SetValue(Ctx_, std::move(*aggregated));
            // Outer code requires window end time (not start as could be expected)
            Self_->Time_->SetValue(Ctx_, NUdf::TUnboxedValuePod((curHopIndex + IntervalHopCount_) * HopTime_));
            Ready_.emplace_back(Self_->OutFinish_->GetValue(Ctx_));
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
                const ui64 intervalHopLimit = Min(IntervalHopCount_, keyState.NextHopIndex - curHopIndex);
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

                for (auto j = futureIt; j != keyState.FutureEvents.end() && j->first - IntervalHopCount_ < curHopIndex; ++j) {
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
                if (curHopIndex < futureIt->first - (IntervalHopCount_ - 1)) {
                    curHopIndex = futureIt->first - (IntervalHopCount_ - 1);
                    if (curHopIndex >= closeBeforeIndex) {
                        break;
                    }
                }

                TMaybe<NUdf::TUnboxedValue> aggregated;
                // Note: overflow impossible:
                // j->first >= futureIt->first since FutureEvents is ordered map
                // futureIt->first >= Buckets.size() >= IntervalHopCount
                for (auto j = futureIt; j != keyState.FutureEvents.end() && j->first - IntervalHopCount_ < curHopIndex; ++j) {
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
            const auto watermarkIndex = watermarkTs / HopTime_;
            EraseNodesIf(StatesMap_, [&](auto& iter) {
                auto& [key, val] = iter;
                ui64 closeBeforeIndex = Max<i64>(watermarkIndex + 1 - IntervalHopCount_, 0);
                const auto keyStateBecameEmpty = CloseOldBucketsForKey(key, val, closeBeforeIndex, newHops, farFutureStateSizeChange);
                if (keyStateBecameEmpty) {
                    key.UnRef();
                }
                return keyStateBecameEmpty;
            });
            return;
        }

        void ClearState() {
            EraseNodesIf(StatesMap_, [&](auto& iter) {
                MKQL_ADD_STAT(Ctx_.Stats, Hop_FarFutureStateSize, -(i64)iter.second.FutureEvents.size());
                iter.first.UnRef();
                return true;
            });
            StatesMap_.rehash(0);
        }

        const NUdf::TUnboxedValue Stream_;
        const TSelf* const Self_;

        const ui64 HopTime_;
        const ui64 IntervalHopCount_;
        const ui64 DelayHopCount_;
        TWatermark& Watermark_;
        bool WatermarkMode_;
        ui64 FarFutureSizeLimit_;
        ui64 FarFutureTimeLimit_;
        EPolicy EarlyPolicy_;
        EPolicy LatePolicy_;
        bool PendingYield_ = false;

        using TStatesMap = std::unordered_map<
            NUdf::TUnboxedValuePod, TKeyState,
            THashFunc, TEqualsFunc,
            TMKQLAllocator<std::pair<const NUdf::TUnboxedValuePod, TKeyState>>>;

        TStatesMap StatesMap_;                  // Map of states for each key
        std::deque<NUdf::TUnboxedValue> Ready_; // buffer for fetching results
        bool Finished_ = false;

        TComputationContext& Ctx_;
        std::optional<TWatermarkTracker> DataWatermarkTracker_;
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
        , Stream_(stream)
        , Item_(item)
        , Key_(key)
        , State_(state)
        , State2_(state2)
        , Time_(time)
        , InSave_(inSave)
        , InLoad_(inLoad)
        , KeyExtract_(keyExtract)
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
        , DataWatermarks_(dataWatermarks)
        , WatermarkMode_(watermarkMode)
        , FarFutureSizeLimit_(farFutureSizeLimit)
        , FarFutureTimeLimitUs_(farFutureTimeLimitUs)
        , EarlyPolicy_(earlyPolicy)
        , LatePolicy_(latePolicy)
        , KeyType_(keyType)
        , StateType_(stateType)
        , KeyPacker_(mutables)
        , StatePacker_(mutables)
        , KeyTypes_()
        , IsTuple_(false)
        , UseIHash_(false)
        , Watermark_(watermark)
    {
        Stateless_ = false;
        bool encoded;
        GetDictionaryKeyTypes(keyType, KeyTypes_, IsTuple_, encoded, UseIHash_);
        Y_ABORT_UNLESS(!encoded, "TODO");
        Equate_ = UseIHash_ ? MakeEquateImpl(KeyType_) : nullptr;
        Hash_ = UseIHash_ ? MakeHashImpl(KeyType_) : nullptr;
    }

    NUdf::TUnboxedValuePod CreateStream(TComputationContext& ctx) const {
        const auto hopTime = Hop_->GetValue(ctx).Get<ui64>();
        const auto interval = Interval_->GetValue(ctx).Get<ui64>();
        const auto delay = Delay_->GetValue(ctx).Get<ui64>();
        const auto dataWatermarks = DataWatermarks_->GetValue(ctx).Get<bool>();
        const auto watermarkMode = WatermarkMode_->GetValue(ctx).Get<bool>();
#define INIT_OPTIONAL_ARGUMENT(var, Type, Member, Default) \
    const auto var = (Member ? Member->GetValue(ctx) : NUdf::TUnboxedValue()).GetOrDefault<Type>(static_cast<Type>(NYql::NHoppingWindow::TSettings{}.Default))
        INIT_OPTIONAL_ARGUMENT(farFutureSizeLimit, ui64, FarFutureSizeLimit_, FarFutureSizeLimit);
        INIT_OPTIONAL_ARGUMENT(farFutureTimeLimitUs, ui64, FarFutureTimeLimitUs_, FarFutureTimeLimit.MicroSeconds());
        INIT_OPTIONAL_ARGUMENT(earlyPolicy, ui32, EarlyPolicy_, EarlyPolicy);
        MKQL_ENSURE(IsIn({static_cast<ui32>(EPolicy::Drop), static_cast<ui32>(EPolicy::Adjust), static_cast<ui32>(EPolicy::Close)}, earlyPolicy),
                    "Unexpected earlyPolicy " << earlyPolicy);
        INIT_OPTIONAL_ARGUMENT(latePolicy, ui32, LatePolicy_, LatePolicy);
        MKQL_ENSURE(IsIn({static_cast<ui32>(EPolicy::Drop), static_cast<ui32>(EPolicy::Adjust)}, latePolicy),
                    "Unexpected latePolicy " << latePolicy);
        MKQL_ENSURE(!(earlyPolicy == static_cast<ui32>(EPolicy::Adjust) && farFutureSizeLimit != Max<ui64>()),
                    "Combination of EarlyPolicy=adjust with SizeLimit is not implemented, please set HoppingWindow SizeLimit to 'max' or use different EarlyPolicy");
#undef INIT_OPTIONAL_ARGUMENT
        const auto intervalHopCount = interval / hopTime;
        const auto delayHopCount = delay / hopTime;

        return ctx.HolderFactory.Create<TStreamValue>(
            Stream_->GetValue(ctx),
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
            TValueHasher(KeyTypes_, IsTuple_, Hash_.Get()),
            TValueEqual(KeyTypes_, IsTuple_, Equate_.Get()),
            Watermark_);
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
        DependsOn(Stream_);
        Own(Item_);
        Own(Key_);
        Own(State_);
        Own(State2_);
        Own(Time_);
        Own(InSave_);
        Own(InLoad_);
        DependsOn(KeyExtract_);
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
        DependsOn(DataWatermarks_);
        DependsOn(WatermarkMode_);
        DependsOn(FarFutureSizeLimit_);
        DependsOn(FarFutureTimeLimitUs_);
        DependsOn(EarlyPolicy_);
        DependsOn(LatePolicy_);
    }

    IComputationNode* const Stream_;

    IComputationExternalNode* const Item_;
    IComputationExternalNode* const Key_;
    IComputationExternalNode* const State_;
    IComputationExternalNode* const State2_;
    IComputationExternalNode* const Time_;
    IComputationExternalNode* const InSave_;
    IComputationExternalNode* const InLoad_;

    IComputationNode* const KeyExtract_;
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
    IComputationNode* const DataWatermarks_;
    IComputationNode* const WatermarkMode_;
    IComputationNode* const FarFutureSizeLimit_;
    IComputationNode* const FarFutureTimeLimitUs_;
    IComputationNode* const EarlyPolicy_;
    IComputationNode* const LatePolicy_;

    TType* const KeyType_;
    TType* const StateType_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> KeyPacker_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> StatePacker_;

    TKeyTypes KeyTypes_;
    bool IsTuple_;
    bool UseIHash_;
    TWatermark& Watermark_;

    NUdf::IEquate::TPtr Equate_;
    NUdf::IHash::TPtr Hash_;
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

} // namespace NKikimr::NMiniKQL
