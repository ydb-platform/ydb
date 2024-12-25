#pragma once

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

template <typename TDerived>
class TAuthScan : public TScanActorBase<TDerived> {
public:
    using TBase = TScanActorBase<TDerived>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TAuthScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, TBase::HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, TBase::HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TAuthScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

protected:
    void ProceedToScan() override {
        TBase::Become(&TAuthScan::StateScan);
        if (TBase::AckReceived) {
            StartScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void StartScan() {
        // TODO: support TableRange filter
        if (auto cellsFrom = TBase::TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.From filter is not supported");
            return;
        }
        if (auto cellsTo = TBase::TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.To filter is not supported");
            return;
        }

        DescribePath(TBase::TenantName);
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to request domain info " << record.GetStatus());
            return;
        }

        if (record.GetPath() != CurrentDescribePath) {
            LOG_WARN_S(ctx, NKikimrServices::SYSTEM_VIEWS,
                "Request " << CurrentDescribePath << " describe but got " << record.GetPath());
            return;
        }
        LOG_TRACE_S(ctx, NKikimrServices::SYSTEM_VIEWS,
            "Got " << record.GetPath() << " describe: " << record.ShortUtf8DebugString());
        CurrentDescribePath = {};
        
        const auto& description = record.GetPathDescription();

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(TBase::ScanId);

        FillBatch(*batch, description);

        TBase::SendBatch(std::move(batch));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        TBase::ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to request domain info");
    }

    void PassAway() override {
        TBase::PassAway();
    }

    void DescribePath(TString path) {
        Y_ABORT_UNLESS(!CurrentDescribePath);
        CurrentDescribePath = path;

        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(path);

        request->Record.MutableOptions()->SetReturnPartitioningInfo(false);
        request->Record.MutableOptions()->SetReturnPartitionConfig(false);
        request->Record.MutableOptions()->SetReturnChildren(false);

        TBase::SendThroughPipeCache(request.Release(), TBase::SchemeShardId);
    }

    virtual void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const ::NKikimrSchemeOp::TPathDescription& description) = 0;

private:
    TString CurrentDescribePath;

};

}
