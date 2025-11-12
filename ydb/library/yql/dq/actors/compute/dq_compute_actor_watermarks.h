#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_watermark_tracker_impl.h>

namespace NYql::NDq {

class TDqComputeActorWatermarks
{
public:
    TDqComputeActorWatermarks(const TString& logPrefix);

    void RegisterAsyncInput(ui64 inputId, TDuration idleTimeout = TDuration::Max()) {
        RegisterAsyncInput(inputId, idleTimeout, TInstant::Now());
    }
    void RegisterInputChannel(ui64 inputId, TDuration idleTimeout = TDuration::Max()) {
        RegisterInputChannel(inputId, idleTimeout, TInstant::Now());
    }

    void RegisterAsyncInput(ui64 inputId, TDuration idleTimeout, TInstant systemTime);
    void RegisterInputChannel(ui64 inputId, TDuration idleTimeout, TInstant systemTime);

    void UnregisterAsyncInput(ui64 inputId);
    void UnregisterInputChannel(ui64 inputId);

    // Will return true, if local watermark inside this async input was moved forward.
    bool NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark) {
        return NotifyAsyncInputWatermarkReceived(inputId, watermark, TInstant::Now());
    }
    bool NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime);

    // Will return true, if local watermark inside this input channel was moved forward.
    bool NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark) {
        return NotifyInChannelWatermarkReceived(inputId, watermark, TInstant::Now());
    }
    bool NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark, TInstant systemTime);

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

private:
    void RegisterInput(ui64 inputId, bool isChannel, TDuration idleTimeout, TInstant systemTime);
    void UnregisterInput(ui64 inputId, bool isChannel);
    bool NotifyInputWatermarkReceived(ui64 inputId, bool isChannel, TInstant watermark, TInstant systemTime);

private:
    TString LogPrefix;

    using TInputKey = std::pair<ui64, bool>;
    TDqWatermarkTrackerImpl<TInputKey> Impl;

    TMaybe<TInstant> PendingWatermark;
    TMaybe<TInstant> MaxWatermark;
};

} // namespace NYql::NDq
