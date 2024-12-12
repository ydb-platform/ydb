#include "sids.h"

#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/library/login/protos/login.pb.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NSysView {

using namespace NSchemeShard;

class TSidsScan : public TScanActorBase<TSidsScan> {
public:
    using TBase = TScanActorBase<TSidsScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TSidsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
        if (auto cellsFrom = TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            From = TString(cellsFrom[0].Data(), cellsFrom[0].Size());
        }
        FromInclusive = TableRange.FromInclusive;

        if (auto cellsTo = TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            To = TString(cellsTo[0].Data(), cellsTo[0].Size());
        }
        ToInclusive = TableRange.ToInclusive;
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            HFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TNodesScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TSidsScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void StartScan() {
        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(TenantName);

        request->Record.MutableOptions()->SetReturnPartitioningInfo(false);
        request->Record.MutableOptions()->SetReturnPartitionConfig(false);
        request->Record.MutableOptions()->SetReturnChildren(false);

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true),
            IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to request domain info " << record.GetStatus());
            return;
        }
        
        const auto& securityState = record.GetPathDescription().GetDomainDescription().GetSecurityState();

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        FillBatch(*batch, securityState);

        batch->Finished = true;

        SendBatch(std::move(batch));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to request domain info");
    }

private:
    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, NLoginProto::TSecurityState securityState) {
        Cerr << securityState.DebugString() << Endl;
        TVector<TCell> cells(::Reserve(Columns.size()));

        // TODO: apply range

        for (const auto& sid : securityState.GetSids()) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::Sids::Name::ColumnId:
                    cells.push_back(TCell(sid.GetName().data(), sid.GetName().size()));
                    break;
                default:
                    cells.emplace_back();
                }
            }
            TArrayRef<const TCell> ref(cells);
            batch.Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }
    }
private:
    TString From;
    bool FromInclusive = false;

    TString To;
    bool ToInclusive = false;
};

THolder<NActors::IActor> CreateSidsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TSidsScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
