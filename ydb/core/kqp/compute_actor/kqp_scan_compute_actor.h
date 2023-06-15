#pragma once
#include "kqp_scan_events.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NKqp::NScanPrivate {

class TKqpScanComputeActor: public NYql::NDq::TDqComputeActorBase<TKqpScanComputeActor> {
private:
    using TBase = NYql::NDq::TDqComputeActorBase<TKqpScanComputeActor>;
    NMiniKQL::TKqpScanComputeContext ComputeCtx;
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta Meta;
    using TBase::TaskRunner;
    using TBase::MemoryLimits;
    using TBase::GetStatsMode;
    using TBase::TxId;
    using TBase::GetTask;
    using TBase::RuntimeSettings;
    using TBase::ContinueExecute;
    std::set<NActors::TActorId> Fetchers;
    NMiniKQL::TKqpScanComputeContext::TScanData* ScanData = nullptr;
    ui64 CalcMkqlMemoryLimit() override {
        return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ChannelBufferSize;
    }
public:
    TKqpScanComputeActor(const TActorId& executerId, ui64 txId,
        NYql::NDqProto::TDqTask* task, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
        TIntrusivePtr<NActors::TProtoArenaHolder> arena);

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvScanExchange::TEvSendData, Handle);
                hFunc(TEvScanExchange::TEvRegisterFetcher, Handle);
                hFunc(TEvScanExchange::TEvFetcherFinished, Handle);
                hFunc(TEvScanExchange::TEvTerminateFromFetcher, Handle)
                default:
                    BaseStateFuncBody(ev);
            }
        } catch (const TMemoryLimitExceededException& e) {
            const TString sInfo = TStringBuilder() << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName() << ", canAllocateExtraMemory: " << CanAllocateExtraMemory;
            CA_LOG_E("ERROR:" + sInfo);
            InternalError(NYql::NDqProto::StatusIds::PRECONDITION_FAILED, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, sInfo);
        } catch (const yexception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::DEFAULT_ERROR, e.what());
        }

        TBase::ReportEventElapsedTime();
    }

    void ProcessRlNoResourceAndDie();

    bool IsQuotingEnabled() const;

    void AcquireRateQuota();

    void FillExtraStats(NYql::NDqProto::TDqComputeActorStats* dst, bool last);

    void HandleEvWakeup(EEvWakeupTag tag);

    void Handle(TEvScanExchange::TEvTerminateFromFetcher::TPtr& ev);

    void Handle(TEvScanExchange::TEvSendData::TPtr& ev);

    void Handle(TEvScanExchange::TEvRegisterFetcher::TPtr& ev);

    void Handle(TEvScanExchange::TEvFetcherFinished::TPtr& ev);

    ui64 CalculateFreeSpace() const {
        return GetMemoryLimits().ChannelBufferSize > ScanData->GetStoredBytes()
            ? GetMemoryLimits().ChannelBufferSize - ScanData->GetStoredBytes()
            : 0ul;
    }

    std::any GetSourcesState() override {
        if (!ScanData) {
            return 0;
        }
        return CalculateFreeSpace();
    }

    void PollSources(std::any prev) override;

    void PassAway() override {
        if (TaskRunner) {
            if (TaskRunner->IsAllocatorAttached()) {
                ComputeCtx.Clear();
            } else {
                auto guard = TaskRunner->BindAllocator(TBase::GetMkqlMemoryLimit());
                ComputeCtx.Clear();
            }
        }

        TBase::PassAway();
    }

    void TerminateSources(const NYql::TIssues& issues, bool success) override {
        if (!ScanData) {
            return;
        }

        for (auto&& i : Fetchers) {
            Send(i, new TEvScanExchange::TEvTerminateFromCompute(success, issues));
        }
    }

    void DoBootstrap();

};

}
