#pragma once

#include "utils.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NSysView {

template <typename TDerived>
class TScanActorBase : public TActorBootstrapped<TDerived> {
public:
    using TBase = TActorBootstrapped<TDerived>;

    TScanActorBase(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : OwnerActorId(ownerId)
        , ScanId(scanId)
        , TableId(tableId)
        , TableRange(tableRange)
        , Columns(columns.begin(), columns.end())
    {}

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::SYSTEM_VIEWS,
            "Scan started, actor: " << TBase::SelfId()
                << ", owner: " << OwnerActorId
                << ", scan id: " << ScanId
                << ", table id: " << TableId);

        auto sysViewServiceId = MakeSysViewServiceID(TBase::SelfId().NodeId());
        TBase::Send(sysViewServiceId, new TEvSysView::TEvGetScanLimiter());

        TBase::Schedule(Timeout, new TEvents::TEvWakeup());
        TBase::Become(&TDerived::StateLimiter);
    }

protected:
    void SendBatch(THolder<NKqp::TEvKqpCompute::TEvScanData> batch) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Sending scan batch, actor: " << TBase::SelfId()
                << ", row count: " << batch->Rows.size()
                << ", finished: " << batch->Finished);

        bool finished = batch->Finished;
        TBase::Send(OwnerActorId, batch.Release());
        if (finished) {
            this->PassAway();
        }
    }

    void HandleTimeout() {
        ReplyErrorAndDie(Ydb::StatusIds::TIMEOUT, "System view: timeout");
    }

    void HandleAbortExecution(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Got abort execution event, actor: " << TBase::SelfId()
                << ", owner: " << OwnerActorId
                << ", scan id: " << ScanId
                << ", table id: " << TableId
                << ", code: " << NYql::NDqProto::StatusIds::StatusCode_Name(ev->Get()->Record.GetStatusCode())
                << ", error: " << ev->Get()->GetIssues().ToOneLineString());

        this->PassAway();
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const TString& message) {
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Scan error, actor: " << TBase::SelfId()
                << ", owner: " << OwnerActorId
                << ", scan id: " << ScanId
                << ", table id: " << TableId
                << ", error: " << message);

        auto error = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>();
        error->Record.SetStatus(status);
        IssueToMessage(NYql::TIssue(message), error->Record.MutableIssues()->Add());

        TBase::Send(OwnerActorId, error.Release());

        this->PassAway();
    }

    void ReplyEmptyAndDie() {
        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;
        TBase::Send(OwnerActorId, batch.Release());

        this->PassAway();
    }

    void PassAway() override {
        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Scan finished, actor: " << TBase::SelfId()
                << ", owner: " << OwnerActorId
                << ", scan id: " << ScanId
                << ", table id: " << TableId);

        if (AllowedByLimiter) {
            ScanLimiter->Dec();
        }

        TBase::PassAway();
    }

    ui64 GetBSControllerId() {
        return MakeBSControllerID();
    }

    template <typename TResponse, typename TEntry, typename TExtractorsMap, bool BatchSupport = false>
    void ReplyBatch(typename TResponse::TPtr& ev) {
        static TExtractorsMap extractors;

        const auto& record = ev->Get()->Record;
        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        TVector<TCell> cells;
        for (const auto& entry : record.GetEntries()) {
            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(entry));
                }
            }

            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        if constexpr (BatchSupport) {
            batch->Finished = record.GetLastBatch();
        } else {
            batch->Finished = true;
        }

        SendBatch(std::move(batch));
    }

private:
    virtual void ProceedToScan() = 0;

    void ReplyLimiterFailedAndDie() {
        ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED, "System view: concurrent scans limit exceeded");
    }

    void ReplyNavigateFailedAndDie() {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "System view: navigate failed");
    }

    void ReplyLookupFailedAndDie() {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "System view: tenant nodes lookup failed");
    }

    void HandleScanAck(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        switch (FailState) {
            case LIMITER_FAILED:
                ReplyLimiterFailedAndDie();
                break;
            case NAVIGATE_FAILED:
                ReplyNavigateFailedAndDie();
                break;
            default:
                AckReceived = true;
                break;
        }
    }

    void HandleLimiter(TEvSysView::TEvGetScanLimiterResult::TPtr& ev) {
        ScanLimiter = ev->Get()->ScanLimiter;

        if (!ScanLimiter->Inc()) {
            FailState = LIMITER_FAILED;
            if (AckReceived) {
                ReplyLimiterFailedAndDie();
            }
            return;
        }

        AllowedByLimiter = true;

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.push_back({});
        auto& entry = request->ResultSet.back();

        entry.TableId = TTableId(TableId.PathId.OwnerId, TableId.PathId.LocalPathId); // domain or subdomain
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;

        TBase::Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        TBase::Become(&TDerived::StateNavigate);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;

        THolder<NSchemeCache::TSchemeCacheNavigate> request(ev->Get()->Request.Release());
        Y_ABORT_UNLESS(request->ResultSet.size() == 1);

        auto& entry = request->ResultSet.back();
        if (entry.Status != TNavigate::EStatus::Ok) {
            FailState = NAVIGATE_FAILED;
            if (AckReceived) {
                ReplyNavigateFailedAndDie();
            }
            return;
        }

        SchemeShardId = entry.DomainInfo->ExtractSchemeShard();

        if (entry.DomainInfo->Params.HasSysViewProcessor()) {
            SysViewProcessorId = entry.DomainInfo->Params.GetSysViewProcessor();
        }

        if (entry.DomainInfo->Params.HasHive()) {
            HiveId = entry.DomainInfo->Params.GetHive();
        } else {
            HiveId = AppData()->DomainsInfo->GetHive();
        }

        DomainKey = entry.DomainInfo->DomainKey;

        TenantName = CanonizePath(entry.Path);

        TBase::Register(CreateTenantNodeEnumerationLookup(TBase::SelfId(), TenantName));
        TBase::Become(&TDerived::StateLookup);
    }

    void HandleLookup(TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
        if (ev->Get()->Success) {
            for (auto& node : ev->Get()->AssignedNodes) {
                TenantNodes.insert(node);
            }
        }

        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Scan prepared, actor: " << TBase::SelfId()
                << ", schemeshard id: " << SchemeShardId
                << ", hive id: " << HiveId
                << ", tenant name: " << TenantName
                << ", domain key: " << DomainKey
                << ", tenant node count: " << TenantNodes.size());

        ProceedToScan();
    }

    STFUNC(StateLimiter) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, HandleScanAck);
            hFunc(TEvSysView::TEvGetScanLimiterResult, HandleLimiter);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, this->PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    STFUNC(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, HandleScanAck);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, this->PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    STFUNC(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, HandleScanAck);
            hFunc(TEvTenantNodeEnumerator::TEvLookupResult, HandleLookup);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, this->PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

protected:
    static constexpr TDuration Timeout = TDuration::Seconds(60);

    const NActors::TActorId OwnerActorId;
    const ui32 ScanId;
    const TTableId TableId;
    TSerializedTableRange TableRange;
    TSmallVec<NMiniKQL::TKqpComputeContextBase::TColumn> Columns;

    ui64 SchemeShardId = 0;
    TPathId DomainKey;
    TString TenantName;
    THashSet<ui32> TenantNodes;
    ui64 HiveId = 0;
    ui64 SysViewProcessorId = 0;

    bool AckReceived = false;

    bool BatchRequestInFlight = false;

private:
    enum EFailState {
        OK,
        LIMITER_FAILED,
        NAVIGATE_FAILED
    } FailState = OK;

    TIntrusivePtr<TScanLimiter> ScanLimiter;
    bool AllowedByLimiter = false;
};


} // NSysView
} // NKikimr
