#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/actors/core/log.h>

namespace NYql::NDq {

class TDqComputeActorWatermarks
{
public:
    TDqComputeActorWatermarks(NActors::TActorIdentity selfId, const TTxId graphId, ui64 taskId);

    void RegisterAsyncInput(ui64 inputId);
    void RegisterInputChannel(ui64 inputId);
    void RegisterOutputChannel(ui64 outputId);
    bool HasOutputChannels() const;

    // Will return true, if local watermark inside this async input was moved forward.
    // CA should pause this async input and wait for coresponding watermarks in all other sources/inputs.
    bool NotifyAsyncInputWatermarkReceived(ui64 inputId, TInstant watermark);

    // Will return true, if local watermark inside this input channel was moved forward.
    // CA should pause this input channel and wait for coresponding watermarks in all other sources/inputs.
    bool NotifyInChannelWatermarkReceived(ui64 inputId, TInstant watermark);

    // Will return true, if watermark was sent to all registered outputs.
    // CA should resume inputs and sources in this case
    bool NotifyOutputChannelWatermarkSent(ui64 outputId, TInstant watermark);

    bool HasPendingWatermark() const;
    TMaybe<TInstant> GetPendingWatermark() const;
    void PopPendingWatermark();

private:
    void RecalcPendingWatermark();
    bool MaybePopPendingWatermark();

private:
    const NActors::TActorIdentity SelfId;
    const TTxId TxId;
    ui64 TaskId;

    std::unordered_map<ui64, TMaybe<TInstant>> AsyncInputsWatermarks;
    std::unordered_map<ui64, TMaybe<TInstant>> InputChannelsWatermarks;
    std::unordered_map<ui64, TMaybe<TInstant>> OutputChannelsWatermarks;

    TMaybe<TInstant> PendingWatermark;
    TMaybe<TInstant> LastWatermark;
};

} // namespace NYql::NDq
