#pragma once

#include "kqp_scan_common.h"
#include "kqp_scan_events.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NKqp::NScanPrivate {

class TKqpScanComputeActor: public NScheduler::TSchedulableComputeActorBase<TKqpScanComputeActor> {
private:
    using TBase = TSchedulableComputeActorBase<TKqpScanComputeActor>;

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
    bool ScanDataInFlight = false;
    ui64 SendDataReceived = 0;
    ui64 AcksSent = 0;

    struct TLockHash {
        size_t operator()(const NKikimrDataEvents::TLock& lock) {
            return MultiHash(
                lock.GetLockId(),
                lock.GetDataShard(),
                lock.GetSchemeShard(),
                lock.GetPathId(),
                lock.GetGeneration(),
                lock.GetCounter(),
                lock.GetHasWrites());
        }
    };

    struct TLockEqual {
        bool operator()(const NKikimrDataEvents::TLock& lhs, const NKikimrDataEvents::TLock& rhs) {
            return lhs.GetLockId() == rhs.GetLockId()
                && lhs.GetDataShard() == rhs.GetDataShard()
                && lhs.GetSchemeShard() == rhs.GetSchemeShard()
                && lhs.GetPathId() == rhs.GetPathId()
                && lhs.GetGeneration() == rhs.GetGeneration()
                && lhs.GetCounter() == rhs.GetCounter()
                && lhs.GetHasWrites() == rhs.GetHasWrites();
        }
    };

    using TLocksHashSet = THashSet<NKikimrDataEvents::TLock, TLockHash, TLockEqual>;

    TLocksHashSet Locks;
    TLocksHashSet BrokenLocks;

    ui64 CalcMkqlMemoryLimit() override {
        return TBase::CalcMkqlMemoryLimit() + ComputeCtx.GetTableScans().size() * MemoryLimits.ChannelBufferSize;
    }

    using EBlockTrackingMode = NKikimrConfig::TTableServiceConfig::EBlockTrackingMode;
    const EBlockTrackingMode BlockTrackingMode;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SCAN_COMPUTE_ACTOR;
    }

    TKqpScanComputeActor(NScheduler::TSchedulableActorOptions schedulableOptions, const TActorId& executerId, ui64 txId,
        NYql::NDqProto::TDqTask* task, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
        TIntrusivePtr<NActors::TProtoArenaHolder> arena, EBlockTrackingMode mode);

    ~TKqpScanComputeActor();

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvScanExchange::TEvSendData, Handle);
                hFunc(TEvScanExchange::TEvRegisterFetcher, Handle);
                hFunc(TEvScanExchange::TEvFetcherFinished, Handle);
                hFunc(TEvScanExchange::TEvTerminateFromFetcher, Handle)
                hFunc(NActors::NMon::TEvHttpInfo, OnMonitoringPage)
                default:
                    BaseStateFuncBody(ev);
            }
        } catch (const TMemoryLimitExceededException& e) {
            TBase::OnMemoryLimitExceptionHandler();
        } catch (const yexception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::DEFAULT_ERROR, e.what());
            FreeComputeCtxData();
        }

        TBase::ReportEventElapsedTime();
    }

    void ProcessRlNoResourceAndDie();

    bool IsQuotingEnabled() const;

    void AcquireRateQuota();

    void FillExtraStats(NYql::NDqProto::TDqComputeActorStats* dst, bool last);

    TMaybe<google::protobuf::Any> ExtraData() override;

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

    ui64 GetSourcesState() {
        if (!ScanData) {
            return 0;
        }
        return CalculateFreeSpace();
    }

    void PollSources(ui64 prevFreeSpace);

    void DoTerminateImpl() override {
        FreeComputeCtxData();
        TBase::DoTerminateImpl();
    }

    void FreeComputeCtxData() {
        if (TaskRunner) {
            if (TaskRunner->IsAllocatorAttached()) {
                ComputeCtx.Clear();
            } else {
                auto guard = TaskRunner->BindAllocator(TBase::GetMkqlMemoryLimit());
                ComputeCtx.Clear();
            }
            ScanData = nullptr;
        }
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

    void ExtraMonitoringInfo(TStringStream& str) override {
        TBase::ExtraMonitoringInfo(str);
        str << Endl << "Backpressure:" << Endl;
        str << "  ScanDataInFlight: " << ScanDataInFlight << Endl;
        if (ScanData) {
            str << "  StoredBytes: " << ScanData->GetStoredBytes() << Endl;
            str << "  FreeSpace: " << CalculateFreeSpace() << Endl;
        }
        str << "  AcksSent: " << AcksSent << Endl;
        str << "  SendDataReceived: " << SendDataReceived << Endl;
        if (!Fetchers.empty()) {
            HTML(str) {
                str << Endl << "Fetcher(s): " << Fetchers.size();
                for (auto& fetcherId : Fetchers) {
                    str << " ";
                    HREF(FetcherLink(SelfId(), fetcherId)) {
                        str << fetcherId;
                    }
                }
                str << Endl;
            }
        }
    }

    void OnMonitoringPage(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        const TCgiParameters& cgi = ev->Get()->Request.GetParams();
        auto sf = cgi.Get("sf");
        if (sf) {
            for (auto& fetcherId : Fetchers) {
                if (sf == ToString(fetcherId)) {
                    TActivationContext::Send(ev->Forward(fetcherId));
                    return;
                }
            }
        }
        TBase::OnMonitoringPage(ev);
    }

};

} // namespace NKikimr::NKqp::NScanPrivate
