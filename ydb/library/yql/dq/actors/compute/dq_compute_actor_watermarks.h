#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/actors/core/log.h>

namespace NYql::NDq {

class TDqComputeActorWatermarks
{
public:
    TDqComputeActorWatermarks(const TString& logPrefix);

    void RegisterAsyncInput(ui64 inputId, TDuration idleDelay = TDuration::Max());
    void RegisterInputChannel(ui64 inputId, TDuration idleDelay = TDuration::Max());

    void UnregisterAsyncInput(ui64 inputId);
    void UnregisterInputChannel(ui64 inputId);

    // Will return true, if local watermark inside this async input was moved forward.
    // CA should pause this async input and wait for coresponding watermarks in all other sources/inputs.
    bool NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark);

    // Will return true, if local watermark inside this input channel was moved forward.
    // CA should pause this input channel and wait for coresponding watermarks in all other sources/inputs.
    bool NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark);

    // Will return true, if pending watermark completed.
    bool NotifyWatermarkWasSent(TInstant watermark);

    bool HasPendingWatermark() const;
    TMaybe<TInstant> GetPendingWatermark() const;
    void PopPendingWatermark();

    // Should be only called only when HasPendingWatermark() is true
    TDuration GetWatermarkDiscrepancy() const;

    TMaybe<TInstant> HandleIdleness(TInstant systemTime);
    TMaybe<TInstant> GetNextIdlenessCheckAt() const;

    void SetLogPrefix(const TString& logPrefix);

private:
    void RegisterInput(ui64 inputId, bool isChannel, TDuration idleDelay);
    void UnregisterInput(ui64 inputId, bool isChannel);
    bool NotifyInputWatermarkReceived(ui64 inputId, bool isChannel, TInstant watermark);

private:
    TString LogPrefix;

    struct TWatermarkState {
        TMaybe<TInstant> Watermark;
        TInstant ExpiresAt;
        TDuration IdleDelay = TDuration::Max();
    };
    THashMap<std::pair<ui64, bool>, TWatermarkState> InputWatermarks;
    template <typename TTimeType, typename TMapType>
    struct TTimeState {
        using TDataIterator = typename TMapType::iterator;
        TTimeType Time;
        TDataIterator Iterator;
        bool operator< (const TTimeState& other) const noexcept {
            // assumes iterator returns stable references
            return Time < other.Time ||
                ( Time == other.Time &&
                  (uintptr_t)&*Iterator < (uintptr_t)&*other.Iterator );
            // 1) there are no `operator<` for hashmap iterator (ForwardIterator);
            // 2) comparing pointers belonging to different allocations is UB;
            // Hence, we use node addresses
            // (we don't actually care about actual ordering of Iterator's, the only
            // requirement it would be stable, unique and passes strict weak ordering;
            // node addresses comparison qualifies)
        }
    };

    using TWatermarksQueueItem = TTimeState<TMaybe<TInstant>, decltype(InputWatermarks)>;
    std::set<TWatermarksQueueItem> WatermarksQueue; // attention: back-references InputWatermarks, must be adjusted with InputWatermarks entries
    using TExpiresQueueItem = TTimeState<TInstant, decltype(InputWatermarks)>;
    std::set<TExpiresQueueItem> ExpiresQueue; // attention: back-references InputWatermarks, must be adjusted with InputWatermarks entries

    TMaybe<TInstant> PendingWatermark;
    TMaybe<TInstant> LastWatermark;
    TMaybe<TInstant> MaxWatermark;
};

} // namespace NYql::NDq
