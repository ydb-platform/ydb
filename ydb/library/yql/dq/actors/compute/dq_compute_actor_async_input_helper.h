#pragma once
#include "dq_compute_actor_async_io.h"
#include "dq_compute_issues_buffer.h"
#include "dq_compute_actor_metrics.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/wilson/wilson_event_budget.h>
#include <ydb/library/yql/dq/runtime/streaming/dq_compute_actor_watermarks.h>

#include <yql/essentials/minikql/mkql_program_builder.h>

//must be included the last
#include "dq_compute_actor_log.h"

namespace NYql::NDq {

constexpr ui32 IssuesBufferSize = 16;

struct TComputeActorAsyncInputHelper {
    TString Type;
    TString LogPrefix;
    ui64 Index;
    IDqComputeActorAsyncInput* AsyncInput = nullptr;
    NActors::IActor* Actor = nullptr;
    TIssuesBuffer IssuesBuffer;
    bool Finished = false;
    const NDqProto::EWatermarksMode WatermarksMode = NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;
    const TDuration WatermarksIdleTimeout = TDuration::Max();
    const NKikimr::NMiniKQL::TType* ValueType = nullptr;
    TMaybe<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;
public:
    TComputeActorAsyncInputHelper(
        const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode,
        TDuration watermarksIdleTimeout)
        : LogPrefix(logPrefix)
        , Index(index)
        , IssuesBuffer(IssuesBufferSize)
        , WatermarksMode(watermarksMode)
        , WatermarksIdleTimeout(watermarksIdleTimeout)
    {}

    virtual i64 GetFreeSpace() const = 0;
    virtual void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, TMaybe<TInstant> watermark, i64 space, bool finished) = 0;

    TMaybe<EResumeSource> PollAsyncInput(TDqComputeActorMetrics& metricsReporter, i64 asyncInputPushLimit,
            NWilson::TSpan* trace = nullptr, NWilson::TSpanEventBudget* events = nullptr) {
        if (Finished) {
            if (trace && events) {
                events->Event(*trace, 10, "async input poll skipped: finished", [&] {
                    NWilson::TKeyValueList attributes;
                    attributes["input_index"] = static_cast<i64>(Index);
                    attributes["poll_performed"] = false;
                    return attributes;
                });
            }
            CA_LOG_T("Skip polling async input[" << Index << "]: finished");
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

            if (space >= freeSpace) {
                CA_LOG_D("Poll async input " << Index << ", stop input by back pressure (initial free space: " << freeSpace << ", read from async input: " << space << " bytes)");
            }

            if (trace && events) {
                events->Event(*trace, 10, "async input poll returned", [&] {
                    NWilson::TKeyValueList attributes;
                    attributes["input_index"] = static_cast<i64>(Index);
                    attributes["poll_performed"] = true;
                    attributes["free_bytes_before_poll"] = freeSpace;
                    attributes["returned_bytes"] = space;
                    attributes["returned_rows"] = static_cast<i64>(batch.RowCount());
                    attributes["finished"] = finished;
                    return attributes;
                });
            }
            metricsReporter.ReportAsyncInputData(Index, batch.RowCount(), space, watermark);

            const bool emptyBatch = batch.empty();
            AsyncInputPush(std::move(batch), watermark, space, finished);
            if (!emptyBatch) {
                // If we have read some data, we must run such reading again
                // to process the case when async input notified us about new data
                // but we haven't read all of it.
                return EResumeSource::CAPollAsync;
            }

        } else {
            if (trace && events) {
                events->Event(*trace, 10, "async input poll skipped: backpressure", [&] {
                    NWilson::TKeyValueList attributes;
                    attributes["input_index"] = static_cast<i64>(Index);
                    attributes["poll_performed"] = false;
                    attributes["free_bytes_before_poll"] = freeSpace;
                    return attributes;
                });
            }
            CA_LOG_T("Skip polling async input[" << Index << "]: no free space: " << freeSpace);
            return EResumeSource::CAPollAsyncNoSpace; // If there is no free space in buffer, => we have something to process
        }
        return {};
    }

    void SetLogPrefix(const TString& logPrefix) {
        LogPrefix = logPrefix;
    }
};

// Used for inputs in Sync ComputeActor and for a base for input transform in both sync and async ComputeActors
struct TComputeActorAsyncInputHelperSync : public TComputeActorAsyncInputHelper {
public:
    using TComputeActorAsyncInputHelper::TComputeActorAsyncInputHelper;

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, TMaybe<TInstant> watermark, i64 space, bool finished) override {
        Buffer->Push(std::move(batch), space);
        if (watermark) {
            Buffer->Push(*watermark);
        }
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
