#pragma once
#include "dq_compute_actor_async_io.h"
#include "dq_compute_issues_buffer.h"
#include "dq_compute_actor_metrics.h"
#include "dq_compute_actor_watermarks.h"

#include <ydb/library/yql/minikql/mkql_program_builder.h>

//must be included the last
#include "dq_compute_actor_log.h"

namespace NYql::NDq {

constexpr ui32 IssuesBufferSize = 16;

struct TComputeActorAsyncInputHelper {
    TString Type;
    const TString LogPrefix;
    ui64 Index;
    IDqComputeActorAsyncInput* AsyncInput = nullptr;
    NActors::IActor* Actor = nullptr;
    TIssuesBuffer IssuesBuffer;
    bool Finished = false;
    const NDqProto::EWatermarksMode WatermarksMode = NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;
    const NKikimr::NMiniKQL::TType* ValueType = nullptr;
    TMaybe<TInstant> PendingWatermark = Nothing();
    TMaybe<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;
public:
    TComputeActorAsyncInputHelper(
        const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode)
        : LogPrefix(logPrefix)
        , Index(index)
        , IssuesBuffer(IssuesBufferSize)
        , WatermarksMode(watermarksMode) {}

    bool IsPausedByWatermark() {
        return PendingWatermark.Defined();
    }

    void Pause(TInstant watermark) {
        YQL_ENSURE(WatermarksMode != NDqProto::WATERMARKS_MODE_DISABLED);
        PendingWatermark = watermark;
    }

    void ResumeByWatermark(TInstant watermark) {
        YQL_ENSURE(watermark == PendingWatermark);
        PendingWatermark = Nothing();
    }

    virtual i64 GetFreeSpace() const = 0;
    virtual void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space, bool finished) = 0;

    TMaybe<EResumeSource> PollAsyncInput(TDqComputeActorMetrics& metricsReporter, TDqComputeActorWatermarks& watermarksTracker, i64 asyncInputPushLimit) {
        if (Finished) {
            CA_LOG_T("Skip polling async input[" << Index << "]: finished");
            return {};
        }

        if (IsPausedByWatermark()) {
            CA_LOG_T("Skip polling async input[" << Index << "]: paused");
            return {};
        }

        const i64 freeSpace = GetFreeSpace();
        if (freeSpace > 0) {
            TMaybe<TInstant> watermark;
            NKikimr::NMiniKQL::TUnboxedValueBatch batch(ValueType);
            Y_ABORT_UNLESS(AsyncInput);
            bool finished = false;
            const i64 space = AsyncInput->GetAsyncInputData(batch, watermark, finished, std::min(freeSpace, asyncInputPushLimit));
            CA_LOG_T("Poll async input " << Index
                << ". Buffer free space: " << freeSpace
                << ", read from async input: " << space << " bytes, "
                << batch.RowCount() << " rows, finished: " << finished);

            metricsReporter.ReportAsyncInputData(Index, batch.RowCount(), space, watermark);

            if (watermark) {
                const auto inputWatermarkChanged = watermarksTracker.NotifyAsyncInputWatermarkReceived(
                    Index,
                    *watermark);

                if (inputWatermarkChanged) {
                    CA_LOG_T("Pause async input " << Index << " because of watermark " << *watermark);
                    Pause(*watermark);
                }
            }
            const bool emptyBatch = batch.empty();
            AsyncInputPush(std::move(batch), space, finished);
            if (!emptyBatch) {
                // If we have read some data, we must run such reading again
                // to process the case when async input notified us about new data
                // but we haven't read all of it.
                return EResumeSource::CAPollAsync;
            }

        } else {
            CA_LOG_T("Skip polling async input[" << Index << "]: no free space: " << freeSpace);
            return EResumeSource::CAPollAsyncNoSpace; // If there is no free space in buffer, => we have something to process
        }
        return {};
    }
};

//Used for inputs in Sync ComputeActor and for a base for input transform in both sync and async ComputeActors
struct TComputeActorAsyncInputHelperSync: public TComputeActorAsyncInputHelper
{
public:
    using TComputeActorAsyncInputHelper::TComputeActorAsyncInputHelper;

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space, bool finished) override {
        Buffer->Push(std::move(batch), space);
        if (finished) {
            Buffer->Finish();
            Finished = true;
        }
    }
    i64 GetFreeSpace() const override{
        return Buffer->GetFreeSpace();
    }

    IDqAsyncInputBuffer::TPtr Buffer;
};

} //namespace NYql::NDq
