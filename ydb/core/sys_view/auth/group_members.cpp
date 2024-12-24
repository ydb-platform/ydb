#include "group_members.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView {

using namespace NSchemeShard;
using namespace NActors;

class TGroupMembersScan : public TScanActorBase<TGroupMembersScan> {
public:
    using TBase = TScanActorBase<TGroupMembersScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TGroupMembersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
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
                    "NSysView::TGroupMembersScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TGroupMembersScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void StartScan() {
        // TODO: support TableRange filter
        if (auto cellsFrom = TableRange.From.GetCells(); cellsFrom.size() > 0 && !cellsFrom[0].IsNull()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.From filter is not supported");
            return;
        }
        if (auto cellsTo = TableRange.To.GetCells(); cellsTo.size() > 0 && !cellsTo[0].IsNull()) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TableRange.To filter is not supported");
            return;
        }

        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(TenantName);

        request->Record.MutableOptions()->SetReturnPartitioningInfo(false);
        request->Record.MutableOptions()->SetReturnPartitionConfig(false);
        request->Record.MutableOptions()->SetReturnChildren(false);

        SendThroughPipeCache(request.Release(), SchemeShardId);
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to request domain info " << record.GetStatus());
            return;
        }
        
        const auto& sids = record.GetPathDescription().GetDomainDescription().GetSecurityState().GetSids();

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        FillBatch(*batch, sids);

        batch->Finished = true;

        SendBatch(std::move(batch));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to request domain info");
    }

    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, google::protobuf::RepeatedPtrField<NLoginProto::TSid> sids) {
        TVector<TCell> cells(::Reserve(Columns.size()));

        // TODO: add rows according to request's sender user rights

        for (const auto& sid : sids) {
            if (sid.GetType() != NLoginProto::ESidType_SidType_GROUP) {
                continue;
            }
            for (const auto& member : sid.GetMembers()) {
                for (auto& column : Columns) {
                    switch (column.Tag) {
                    case Schema::GroupMembers::GroupSid::ColumnId:
                        cells.push_back(TCell(sid.GetName().data(), sid.GetName().size()));
                        break;
                    case Schema::GroupMembers::MemberSid::ColumnId:
                        cells.push_back(TCell(member.data(), member.size()));
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
    }
};

THolder<NActors::IActor> CreateGroupMembersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TGroupMembersScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
