#include "create_table_formatter.h"
#include "show_create.h"

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

class TShowCreate : public TScanActorBase<TShowCreate> {
public:
    using TBase  = TScanActorBase<TShowCreate>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TShowCreate(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
        , Database(database)
        , UserToken(std::move(userToken))
    {
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

    void HandleLimiter(TEvSysView::TEvGetScanLimiterResult::TPtr& ev) override {
        ScanLimiter = ev->Get()->ScanLimiter;

        if (!ScanLimiter->Inc()) {
            FailState = LIMITER_FAILED;
            if (AckReceived) {
                ReplyLimiterFailedAndDie();
            }
            return;
        }

        AllowedByLimiter = true;

        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Scan prepared, actor: " << TBase::SelfId());

        ProceedToScan();
        return;
    }

    void StartScan() {
        if (!AppData()->FeatureFlags.GetEnableShowCreate()) {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                TStringBuilder() << "Sys view is not supported: " << ShowCreateName);
            return;
        }

        const auto& cellsFrom = TableRange.From.GetCells();

        if (cellsFrom.size() != 2 || cellsFrom[0].IsNull() || cellsFrom[1].IsNull()) {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, "Invalid read key");
            return;
        }

        if (!TableRange.To.GetCells().empty()) {
            const auto& cellsTo = TableRange.To.GetCells();
            if (cellsTo.size() != 2 || cellsTo[0].IsNull() || cellsTo[1].IsNull()) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, "Invalid read key");
                return;
            }

            if (cellsFrom[0].AsBuf() != cellsTo[0].AsBuf() || cellsFrom[1].AsBuf() != cellsTo[1].AsBuf()) {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, "Invalid table range");
                return;
            }
        }

        Path = cellsFrom[0].AsBuf();
        PathType = cellsFrom[1].AsBuf();

        if (PathType != "Table") {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, TStringBuilder() << "Invalid path type: " << PathType);
            return;
        }

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        navigateRequest->Record.SetDatabaseName(Database);
        if (UserToken) {
            navigateRequest->Record.SetUserToken(UserToken->GetSerializedToken());
        }
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(Path);
        record->MutableOptions()->SetReturnBoundaries(true);
        record->MutableOptions()->SetShowPrivateTable(false);

        Send(MakeTxProxyID(), navigateRequest.release());
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void ProceedToScan() override {
        Become(&TShowCreate::StateWork);
        if (AckReceived) {
            StartScan();
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        std::optional<TString> path;
        std::optional<TString> statement;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeTable) {
                    ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, "Invalid path type");
                    return;
                }

                const auto& tableDesc = pathDescription.GetTable();

                std::pair<TString, TString> pathPair;

                {
                    TString error;
                    if (!TrySplitPathByDb(Path, Database, pathPair, error)) {
                        ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                        return;
                    }
                }

                auto [_, tablePath] = pathPair;
                bool temporary = false;

                if (NKqp::IsSessionsDirPath(Database, tablePath)) {
                    auto pathVecTmp = SplitPath(tablePath);
                    auto sz = pathVecTmp.size();
                    Y_ENSURE(sz > 3  && pathVecTmp[0] == ".tmp" && pathVecTmp[1] == "sessions");

                    auto pathTmp = JoinPath(TVector<TString>(pathVecTmp.begin() + 3, pathVecTmp.end()));
                    std::pair<TString, TString> pathPairTmp;
                    TString error;
                    if (!TrySplitPathByDb(pathTmp, Database, pathPairTmp, error)) {
                        ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                        return;
                    }

                    tablePath = pathPairTmp.second;
                    temporary = true;
                }

                TCreateTableFormatter formatter;
                auto formatterResult = formatter.Format(tablePath, tableDesc, temporary);
                if (formatterResult.IsSuccess()) {
                    path = tablePath;
                    statement = formatterResult.ExtractOut();
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

        Y_ENSURE(path.has_value());
        Y_ENSURE(statement.has_value());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        TVector<TCell> cells;
        for (auto column : Columns) {
            switch (column.Tag) {
                case Schema::ShowCreate::Path::ColumnId: {
                    cells.emplace_back(TCell(path.value().data(), path.value().size()));
                    break;
                }
                case Schema::ShowCreate::PathType::ColumnId: {
                    cells.emplace_back(TCell(PathType.data(), PathType.size()));
                    break;
                }
                case Schema::ShowCreate::Statement::ColumnId: {
                    cells.emplace_back(TCell(statement.value().data(), statement.value().size()));
                    break;
                }
                default:
                    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected column tag");
                    return;
            }
        }

        Y_ENSURE(cells.size() == 3);
        TArrayRef<const TCell> resultRow(cells);
        batch->Rows.emplace_back(TOwnedCellVec::Make(resultRow));
        batch->Finished = true;

        SendBatch(std::move(batch));
    }

private:
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Path;
    TString PathType;
};

}

THolder<NActors::IActor> CreateShowCreate(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TShowCreate>(ownerId, scanId, tableId, tableRange, columns, database, std::move(userToken));
}

} // NSysView
} // NKikimr
