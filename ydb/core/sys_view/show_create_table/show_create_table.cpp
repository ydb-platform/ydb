#include "create_table_formatter.h"
#include "show_create_table.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>


namespace NKikimr {
namespace NSysView {

namespace {

using namespace NActors;

class TShowCreateTable : public TScanActorBase<TShowCreateTable> {
public:
    using TBase  = TScanActorBase<TShowCreateTable>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TShowCreateTable(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
        , Database(database)
        , UserToken(std::move(userToken))
    {
        const auto& cellsFrom = TableRange.From.GetCells();
        Y_ENSURE(cellsFrom.size() == 1 && !cellsFrom[0].IsNull());

        TablePath = cellsFrom[0].AsBuf();
    }

    STFUNC(StateWork) {
       switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TScanActorBase: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void StartScan() {
        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        navigateRequest->Record.SetDatabaseName(Database);
        if (UserToken) {
            navigateRequest->Record.SetUserToken(UserToken->GetSerializedToken());
        }
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(TablePath);
        record->MutableOptions()->SetReturnBoundaries(true);
        record->MutableOptions()->SetShowPrivateTable(false);

        Send(MakeTxProxyID(), navigateRequest.release());

        Become(&TShowCreateTable::StateWork);
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void ProceedToScan() override {
        Become(&TShowCreateTable::StateWork);
        if (AckReceived) {
            StartScan();
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        std::optional<TString> out;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                Y_ENSURE(pathDescription.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeTable);

                const auto& tableDesc = pathDescription.GetTable();

                TCreateTableFormatter formatter;
                auto formatterResult = formatter.Format(tableDesc);
                if (formatterResult.IsSuccess()) {
                    out = formatterResult.ExtractOut();
                } else {
                    ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
                    return;
                }
                break;
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, record.GetReason());
                return;
            }
            case NKikimrScheme::StatusAccessDenied: {
                ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, record.GetReason());
                return;
            }
            case NKikimrScheme::StatusNotAvailable: {
                ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, record.GetReason());
                return;
            }
            default: {
                ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, record.GetReason());
                return;
            }
        }

        Y_ENSURE(out.has_value());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        TVector<TCell> cells;
        cells.emplace_back(TCell(TablePath.data(), TablePath.size()));
        cells.emplace_back(TCell(out.value().data(), out.value().size()));
        TArrayRef<const TCell> resultRow(cells);
        batch->Rows.emplace_back(TOwnedCellVec::Make(resultRow));
        batch->Finished = true;

        SendBatch(std::move(batch));
    }

private:
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString TablePath;
};

}

THolder<NActors::IActor> CreateShowCreateTable(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TShowCreateTable>(ownerId, scanId, tableId, tableRange, columns, database, std::move(userToken));
}

} // NSysView
} // NKikimr
