#include "create_table_formatter.h"
#include "create_view_formatter.h"
#include "show_create.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>


namespace NKikimr {
namespace NSysView {

namespace {

using namespace NActors;

TString ToString(NKikimrSchemeOp::EPathType pathType) {
    switch (pathType) {
        case NKikimrSchemeOp::EPathTypeTable:
        case NKikimrSchemeOp::EPathTypeColumnTable:
            return "Table";
        case NKikimrSchemeOp::EPathTypeView:
            return "View";
        default:
            Y_ENSURE(false, "No user-friendly name for a path type: " << pathType);
            return "";
    }
}

bool RewriteTemporaryTablePath(const TString& database, TString& tablePath, TString& error) {
    auto pathVecTmp = SplitPath(tablePath);
    auto sz = pathVecTmp.size();
    Y_ENSURE(sz > 3  && pathVecTmp[0] == ".tmp" && pathVecTmp[1] == "sessions");

    auto pathTmp = JoinPath(TVector<TString>(pathVecTmp.begin() + 3, pathVecTmp.end()));
    std::pair<TString, TString> pathPairTmp;
    if (!TrySplitPathByDb(pathTmp, database, pathPairTmp, error)) {
        return false;
    }

    tablePath = pathPairTmp.second;
    return true;
}

class TShowCreate : public TScanActorBase<TShowCreate> {
public:
    using TBase  = TScanActorBase<TShowCreate>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TShowCreate(const NActors::TActorId& ownerId, ui32 scanId,
        const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : TBase(ownerId, scanId, sysViewInfo, tableRange, columns)
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

    STFUNC(StateCollectTableSettings) {
        switch (ev->GetTypeRewrite()) {
             hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleCollectTableSettings);
             hFunc(NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult, Handle);
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
                TStringBuilder() << "Sys view 'show_create' is not supported");
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

        if (!IsIn({"Table", "View"}, PathType)) {
            return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                << "Unsupported path type: " << PathType
            );
        }

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        navigateRequest->Record.SetDatabaseName(Database);
        if (UserToken) {
            navigateRequest->Record.SetUserToken(UserToken->GetSerializedToken());
        }
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(Path);
        if (PathType == "Table") {
            record->MutableOptions()->SetReturnBoundaries(true);
            record->MutableOptions()->SetShowPrivateTable(false);
            record->MutableOptions()->SetReturnBoundaries(true);
            record->MutableOptions()->SetShowPrivateTable(true);
            record->MutableOptions()->SetReturnIndexTableBoundaries(true);
            record->MutableOptions()->SetReturnPartitionConfig(true);
        }

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

    bool NeedToCollectTableSettings(const NKikimrSchemeOp::TTableDescription& tableDesc) {
        return !tableDesc.GetCdcStreams().empty() || !tableDesc.GetSequences().empty();
    }

    void StartCollectTableSettings(const TString& tablePath, const NKikimrSchemeOp::TTableDescription& tableDesc, bool temporary) {
        CollectTableSettingsState = MakeHolder<TCollectTableSettingsState>();
        CollectTableSettingsState->TablePath = tablePath;
        CollectTableSettingsState->TableDescription = tableDesc;
        CollectTableSettingsState->Temporary = temporary;

        for (const auto& cdcStream: tableDesc.GetCdcStreams()) {
            std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
            navigateRequest->Record.SetDatabaseName(Database);
            if (UserToken) {
                navigateRequest->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();

            auto persQueuePath = JoinPath({Path, cdcStream.GetName(), "streamImpl"});

            CollectTableSettingsState->PersQueues[JoinPath({tablePath, cdcStream.GetName(), "streamImpl"})] = nullptr;

            record->SetPath(persQueuePath);
            record->MutableOptions()->SetReturnBoundaries(true);
            record->MutableOptions()->SetShowPrivateTable(true);
            record->MutableOptions()->SetReturnIndexTableBoundaries(true);
            record->MutableOptions()->SetReturnPartitionConfig(true);
            record->MutableOptions()->SetReturnPartitioningInfo(false);

            Send(MakeTxProxyID(), navigateRequest.release());
        }

        for (const auto& sequence: tableDesc.GetSequences()) {
            auto sequencePathId = TPathId::FromProto(sequence.GetPathId());
            CollectTableSettingsState->Sequences[sequencePathId] = nullptr;

            Send(NSequenceProxy::MakeSequenceProxyServiceID(),
                new NSequenceProxy::TEvSequenceProxy::TEvGetSequence(Database, sequencePathId)
            );
        }
    }

    void FillBatch(NKqp::TEvKqpCompute::TEvScanData& batch, const TString& path, const TString& createQuery) {
        TVector<TCell> cells;
        for (auto column : Columns) {
            switch (column.Tag) {
                case Schema::ShowCreate::Path::ColumnId: {
                    cells.emplace_back(TCell(path.data(), path.size()));
                    break;
                }
                case Schema::ShowCreate::PathType::ColumnId: {
                    cells.emplace_back(TCell(PathType.data(), PathType.size()));
                    break;
                }
                case Schema::ShowCreate::CreateQuery::ColumnId: {
                    cells.emplace_back(TCell(createQuery.data(), createQuery.size()));
                    break;
                }
                default:
                    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected column tag");
                    return;
            }
        }

        Y_ENSURE(cells.size() == 3);
        TArrayRef<const TCell> resultRow(cells);
        batch.Rows.emplace_back(TOwnedCellVec::Make(resultRow));
        batch.Finished = true;
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        std::optional<TString> path;
        std::optional<TString> createQuery;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                if (auto pathType = ToString(pathDescription.GetSelf().GetPathType()); pathType != PathType) {
                    return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                        << "Path type mismatch, expected: " << PathType << ", found: " << pathType
                    );
                }

                std::pair<TString, TString> pathPair;
                {
                    TString error;
                    if (!TrySplitPathByDb(Path, Database, pathPair, error)) {
                        ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                        return;
                    }
                }

                switch (pathDescription.GetSelf().GetPathType()) {
                    case NKikimrSchemeOp::EPathTypeTable: {
                        const auto& tableDesc = pathDescription.GetTable();
                        auto tablePath = pathPair.second;

                        bool temporary = false;
                        if (NKqp::IsSessionsDirPath(Database, pathPair.second)) {
                            TString error;
                            if (!RewriteTemporaryTablePath(Database, tablePath, error)) {
                                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                            }
                            temporary = true;
                        }

                        if (NeedToCollectTableSettings(tableDesc)) {
                            StartCollectTableSettings(tablePath, tableDesc, temporary);
                            Become(&TShowCreate::StateCollectTableSettings);
                            return;
                        }

                        TCreateTableFormatter formatter;
                        auto formatterResult = formatter.Format(tablePath, Path, tableDesc, temporary, {}, {});
                        if (formatterResult.IsSuccess()) {
                            path = tablePath;
                            createQuery = formatterResult.ExtractOut();
                        } else {
                            ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
                            return;
                        }
                        break;
                    }
                    case NKikimrSchemeOp::EPathTypeColumnTable: {
                        const auto& columnTableDesc = pathDescription.GetColumnTableDescription();
                        auto tablePath = pathPair.second;

                        bool temporary = false;
                        if (NKqp::IsSessionsDirPath(Database, pathPair.second)) {
                            TString error;
                            if (!RewriteTemporaryTablePath(Database, tablePath, error)) {
                                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                            }
                            temporary = true;
                        }

                        TCreateTableFormatter formatter;
                        auto formatterResult = formatter.Format(tablePath, Path, columnTableDesc, temporary);
                        if (formatterResult.IsSuccess()) {
                            path = tablePath;
                            createQuery = formatterResult.ExtractOut();
                        } else {
                            ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
                            return;
                        }
                        break;
                    }
                    case NKikimrSchemeOp::EPathTypeView: {
                        const auto& description = pathDescription.GetViewDescription();
                        path = pathPair.second;

                        TCreateViewFormatter formatter;
                        auto formatterResult = formatter.Format(*path, Path, description);
                        if (formatterResult.IsSuccess()) {
                            createQuery = formatterResult.ExtractOut();
                        } else {
                            return ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
                        }
                        break;
                    }
                    default: {
                        return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                            << "Unsupported path type: " << pathDescription.GetSelf().GetPathType()
                        );
                    }
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
        Y_ENSURE(createQuery.has_value());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        FillBatch(*batch, path.value(), createQuery.value());

        SendBatch(std::move(batch));
    }

    void HandleCollectTableSettings(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        Y_ENSURE(CollectTableSettingsState);
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        std::optional<TString> path;
        std::optional<TString> createQuery;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                auto currentPath = record.GetPath();
                const auto& pathDescription = record.GetPathDescription();

                std::pair<TString, TString> pathPair;
                {
                    TString error;
                    if (!TrySplitPathByDb(currentPath, Database, pathPair, error)) {
                        ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, error);
                        return;
                    }
                }

                switch (pathDescription.GetSelf().GetPathType()) {
                    case NKikimrSchemeOp::EPathTypePersQueueGroup: {
                        const auto& description = pathDescription.GetPersQueueGroup();

                        auto it = CollectTableSettingsState->PersQueues.find(pathPair.second);
                        if (it == CollectTableSettingsState->PersQueues.end() || it->second) {
                            return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected topic path");
                        }
                        it->second = MakeHolder<NKikimrSchemeOp::TPersQueueGroupDescription>(description);
                        CollectTableSettingsState->CurrentPersQueuesNumber++;

                        if (!CollectTableSettingsState->IsReady()) {
                            return;
                        }

                        TCreateTableFormatter formatter;
                        auto formatterResult = formatter.Format(
                            CollectTableSettingsState->TablePath,
                            Path,
                            CollectTableSettingsState->TableDescription,
                            CollectTableSettingsState->Temporary,
                            CollectTableSettingsState->PersQueues,
                            CollectTableSettingsState->Sequences
                        );
                        if (formatterResult.IsSuccess()) {
                            path = CollectTableSettingsState->TablePath;
                            createQuery = formatterResult.ExtractOut();
                        } else {
                            ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
                            return;
                        }
                        break;
                    }
                    default: {
                        return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
                            << "Unsupported path type");
                    }
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
        Y_ENSURE(createQuery.has_value());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        FillBatch(*batch, path.value(), createQuery.value());

        SendBatch(std::move(batch));
    }

    void Handle(NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyErrorAndDie(ev->Get()->Status, ev->Get()->Issues.ToString());
            return;
        }

        auto* msg = ev->Get();

        auto it = CollectTableSettingsState->Sequences.find(msg->PathId);
        if (it == CollectTableSettingsState->Sequences.end()) {
            return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unknown sequence path id: " << msg->PathId);
        }
        if (it->second) {
            return ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Found duplicate sequence path id: " << msg->PathId);
        }
        it->second = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>(*msg);
        CollectTableSettingsState->CurrentSequencesNumber++;

        if (!CollectTableSettingsState->IsReady()) {
            return;
        }

        TCreateTableFormatter formatter;
        auto formatterResult = formatter.Format(
            CollectTableSettingsState->TablePath,
            Path,
            CollectTableSettingsState->TableDescription,
            CollectTableSettingsState->Temporary,
            CollectTableSettingsState->PersQueues,
            CollectTableSettingsState->Sequences
        );
        std::optional<TString> path;
        std::optional<TString> createQuery;
        if (formatterResult.IsSuccess()) {
            path = CollectTableSettingsState->TablePath;
            createQuery = formatterResult.ExtractOut();
        } else {
            ReplyErrorAndDie(formatterResult.GetStatus(), formatterResult.GetError());
            return;
        }

        Y_ENSURE(path.has_value());
        Y_ENSURE(createQuery.has_value());

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);

        FillBatch(*batch, path.value(), createQuery.value());

        SendBatch(std::move(batch));
    }

private:
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Path;
    TString PathType;

    struct TCollectTableSettingsState {
        TString TablePath;
        NKikimrSchemeOp::TTableDescription TableDescription;
        bool Temporary;
        THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>> PersQueues;
        ui32 CurrentPersQueuesNumber = 0;
        THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>> Sequences;
        ui32 CurrentSequencesNumber = 0;

        bool IsReady() const {
            return CurrentPersQueuesNumber == PersQueues.size() && CurrentSequencesNumber == Sequences.size();
        }
    };
    THolder<TCollectTableSettingsState> CollectTableSettingsState;
};

}

THolder<NActors::IActor> CreateShowCreate(const NActors::TActorId& ownerId, ui32 scanId,
    const NKikimrSysView::TSysViewDescription& sysViewInfo, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
{
    return MakeHolder<TShowCreate>(ownerId, scanId, sysViewInfo, tableRange, columns, database,
        std::move(userToken));
}

} // NSysView
} // NKikimr
