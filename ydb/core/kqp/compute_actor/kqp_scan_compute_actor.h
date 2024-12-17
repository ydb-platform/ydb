#pragma once
#include "kqp_scan_events.h"

#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/core/kqp/runtime/kqp_compute_scheduler.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NKqp::NScanPrivate {

class TKqpScanComputeActor: public TSchedulableComputeActorBase<TKqpScanComputeActor> {
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
    const TMaybe<ui64> LockTxId;
    const ui32 LockNodeId;

    struct TLockHash {
        bool operator()(const NKikimrDataEvents::TLock& lock) {
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

    TKqpScanComputeActor(TComputeActorSchedulingOptions, const TActorId& executerId, ui64 txId, TMaybe<ui64> lockTxId, ui32 lockNodeId,
        NYql::NDqProto::TDqTask* task, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
        TIntrusivePtr<NActors::TProtoArenaHolder> arena, EBlockTrackingMode mode);

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
            TBase::OnMemoryLimitExceptionHandler();
        } catch (const yexception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, NYql::TIssuesIds::DEFAULT_ERROR, e.what());
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
