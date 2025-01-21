#include "auth_scan_base.h"
#include "users.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/login/protos/login.pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSysView::NAuth {

using namespace NSchemeShard;
using namespace NActors;

class TUsersScan : public TScanActorBase<TUsersScan> {
public:
    using TBase = TScanActorBase<TUsersScan>;

    TUsersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
    {
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvListUsersResult, Handle);
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, TBase::HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, TBase::HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::NAuth::TUsersScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

protected:
    void ProceedToScan() override {
        TBase::Become(&TUsersScan::StateScan);
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

        auto request = MakeHolder<TEvSchemeShard::TEvListUsers>();

        LOG_TRACE_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Sending list users request " << request->Record.ShortUtf8DebugString());

        TBase::SendThroughPipeCache(request.Release(), TBase::SchemeShardId);
    }

    void Handle(TEvSchemeShard::TEvListUsersResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        LOG_TRACE_S(ctx, NKikimrServices::SYSTEM_VIEWS,
            "Got list users response " <<   record.ShortUtf8DebugString());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(TBase::ScanId);

        FillBatch(*batch, record);

        TBase::SendBatch(std::move(batch));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        TBase::ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Failed to request domain info");
    }

    void PassAway() override {
        TBase::PassAway();
    }

    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const NKikimrScheme::TEvListUsersResult& result) {
        TVector<TCell> cells(::Reserve(Columns.size()));

        // TODO: add rows according to request's sender user rights

        for (const auto& user : result.GetUsers()) {
            for (auto& column : Columns) {
                switch (column.Tag) {
                case Schema::AuthUsers::Sid::ColumnId:
                    cells.push_back(user.HasName()
                        ? TCell(user.GetName().data(), user.GetName().size())
                        : TCell());
                    break;
                case Schema::AuthUsers::IsEnabled::ColumnId:
                    cells.push_back(user.HasIsEnabled()
                        ? TCell::Make(user.GetIsEnabled())
                        : TCell());
                    break;
                case Schema::AuthUsers::IsLockedOut::ColumnId:
                    cells.push_back(user.HasIsLockedOut()
                        ? TCell::Make(user.GetIsLockedOut())
                        : TCell());
                    break;
                case Schema::AuthUsers::CreatedAt::ColumnId:
                    cells.push_back(user.HasCreatedAt()
                        ? TCell::Make(user.GetCreatedAt())
                        : TCell());
                    break;
                case Schema::AuthUsers::LastSuccessfulAttemptAt::ColumnId:
                    cells.push_back(user.HasLastSuccessfulAttemptAt()
                        ? TCell::Make(user.GetLastSuccessfulAttemptAt())
                        : TCell());
                    break;
                case Schema::AuthUsers::LastFailedAttemptAt::ColumnId:
                    cells.push_back(user.HasLastFailedAttemptAt()
                        ? TCell::Make(user.GetLastFailedAttemptAt())
                        : TCell());
                    break;
                case Schema::AuthUsers::FailedAttemptCount::ColumnId:
                    cells.push_back(user.HasFailedAttemptCount()
                        ? TCell::Make(user.GetFailedAttemptCount())
                        : TCell());
                    break;
                case Schema::AuthUsers::PasswordHash::ColumnId:
                    cells.push_back(user.HasPasswordHash()
                        ? TCell(user.GetPasswordHash().data(), user.GetPasswordHash().size())
                        : TCell());
                    break;
                default:
                    cells.emplace_back();
                }
            }

            TArrayRef<const TCell> ref(cells);
            batch.Rows.emplace_back(TOwnedCellVec::Make(ref));
            cells.clear();
        }

        batch.Finished = true;
    }
};

THolder<NActors::IActor> CreateUsersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    return MakeHolder<TUsersScan>(ownerId, scanId, tableId, tableRange, columns);
}

}
