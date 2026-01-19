#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_watermark_tracker_impl.h>

namespace NYql::NDq::NDqComputeActorWatermarksImpl {
    struct TInputKey {
        ui64 InputId;
        bool IsChannel;

        constexpr auto operator<=> (const TInputKey&) const = default;
    };
} // namespace NYql::NDq::NDqComputeActorWatermarksImpl

template<>
struct THash<NYql::NDq::NDqComputeActorWatermarksImpl::TInputKey> {
    constexpr size_t operator() (const auto& x) const noexcept {
        return (x.InputId << 1) ^ x.IsChannel; // better than CombineHashes for this particular purpose
    }
};

namespace NYql::NDq {
class TDqComputeActorWatermarks
{
public:
    TDqComputeActorWatermarks(const TString& logPrefix);

    void RegisterAsyncInput(ui64 inputId, TDuration idleTimeout = TDuration::Max(), TInstant systemTime = TInstant::Now());
    void RegisterInputChannel(ui64 inputId, TDuration idleTimeout = TDuration::Max(), TInstant systemTime = TInstant::Now());

    void UnregisterAsyncInput(ui64 inputId, bool silent = false);
    void UnregisterInputChannel(ui64 inputId, bool silent = false);

    // Will return true, if local watermark inside this async input was moved forward.
    bool NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime = TInstant::Now());

    // Will return true, if local watermark inside this input channel was moved forward.
    bool NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime = TInstant::Now());

    // Will return true, if pending watermark completed.
    bool NotifyWatermarkWasSent(TInstant watermark);

    bool HasPendingWatermark() const;
    TMaybe<TInstant> GetPendingWatermark() const;
    void PopPendingWatermark();

    TMaybe<TInstant> GetMaxWatermark() const;

    // Return watermark that was generated after input idleness processing
    TMaybe<TInstant> HandleIdleness(TInstant systemTime);

    // Return idleness check that should be scheduled or Nothing()
    [[nodiscard]] TMaybe<TInstant> PrepareIdlenessCheck();
    // Return true if idleness check should be performed
    [[nodiscard]] bool ProcessIdlenessCheck(TInstant notifyTime);

    void SetLogPrefix(const TString& logPrefix);

    void Out(IOutputStream& str) const;

    void RegisterInput(ui64 inputId, bool isChannel, TDuration idleTimeout = TDuration::Max(), TInstant systemTime = TInstant::Now());
    void UnregisterInput(ui64 inputId, bool isChannel, bool silent = false);
    bool NotifyInputWatermarkReceived(ui64 inputId, bool isChannel, TInstant watermark, TInstant systemTime = TInstant::Now());

private:
    TString LogPrefix;

    using TInputKey = NDqComputeActorWatermarksImpl::TInputKey;
    TDqWatermarkTrackerImpl<TInputKey> Impl;

    TMaybe<TInstant> PendingWatermark;
    TMaybe<TInstant> MaxWatermark;
};

} // namespace NYql::NDq
