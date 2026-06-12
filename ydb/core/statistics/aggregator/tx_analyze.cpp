#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyze : public TTxBase {
    TEvStatistics::TEvAnalyze::TPtr Event;
    TActorId ReplyToActorId;
    // When set, Complete() replays the terminal status to the sender instead of
    // scheduling a new traversal. Populated when a retry arrives for an operation
    // that already finished.
    std::optional<NKikimrStat::TEvAnalyzeResponse::EStatus> TerminalReplay;
    NYql::TIssues TerminalReplayIssues;

    TTxAnalyze(TSelf* self, TEvStatistics::TEvAnalyze::TPtr ev)
        : TTxBase(self)
        , Event(std::move(ev))
        , ReplyToActorId(Event->Sender)
    {}

    const NKikimrStat::TEvAnalyze& Record() const { return Event->Get()->Record; }

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. ReplyToActorId " << ReplyToActorId << " , Record " << Record());

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const TString operationId = Record().GetOperationId();

        // Check existing force traversal with the same (OperationId, DatabaseName).
        // A serverless shared SA can see the same opId from different tenants. Only
        // treat the existing op as "the same one" when both opId and database match.
        // If the opId matches but the database doesn't, drop the older entry — keeping
        // two ops with the same opId in ForceTraversals would corrupt every internal
        // opId-only lookup (Current*, MarkFinished, etc.).
        auto* existingOperation = Self->ForceTraversalOperation(operationId);
        if (existingOperation && existingOperation->DatabaseName != Record().GetDatabase()) {
            SA_LOG_W("[" << Self->TabletID() << "] TTxAnalyze::Execute. Replacing "
                "force traversal with same OperationId " << operationId.Quote() << " from "
                "different database (was " << existingOperation->DatabaseName.Quote()
                << ", now " << Record().GetDatabase().Quote() << ")");
            Self->DeleteForceTraversalOperation(operationId, db);
            existingOperation = nullptr;
        }

        // update existing force traversal
        if (existingOperation) {
            if (IsTerminalAnalyzeState(existingOperation->State)) {
                // Idempotent retry: the operation already finished. Don't redo the
                // analyze; replay the cached terminal status in Complete() so the
                // requester sees a stable result and the history entry is preserved.
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Replay terminal response. OperationId " << operationId.Quote() << " , ReplyToActorId " << ReplyToActorId);
                switch (existingOperation->State) {
                    case Ydb::Table::AnalyzeState::STATE_DONE:
                        TerminalReplay = NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS;
                        break;
                    case Ydb::Table::AnalyzeState::STATE_CANCELLED:
                        TerminalReplay = NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED;
                        break;
                    default: // STATE_FAILED
                        TerminalReplay = NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR;
                        break;
                }
                TerminalReplayIssues = existingOperation->Issues;
                return true;
            } else if (existingOperation->ReplyToActorId == Event->Sender) {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Reattach to existing force traversal. OperationId " << operationId.Quote() << " , ReplyToActorId " << ReplyToActorId);
                existingOperation->RequestingActorReattached = true;
                return true;
            } else {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Delete broken force traversal. OperationId " << operationId.Quote() << " , ReplyToActorId " << ReplyToActorId);
                Self->DeleteForceTraversalOperation(operationId, db);
            }
        }

        const TString types = JoinVectorIntoString(TVector<ui32>(Record().GetTypes().begin(), Record().GetTypes().end()), ",");
        const TString& databaseName = Record().GetDatabase();

        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Create new force traversal operation"
            << ", OperationId: " << operationId.Quote()
            << ", DatabaseName: `" << databaseName << "'"
            << ", Types: " << types);

        // create new force traversal
        auto createdAt = ctx.Now();
        TForceTraversalOperation operation {
            .OperationId = operationId,
            .DatabaseName = databaseName,
            .Tables = {},
            .Types = types,
            .ReplyToActorId = ReplyToActorId,
            .CreatedAt = createdAt
        };

        for (const auto& table : Record().GetTables()) {
            const TPathId pathId = TPathId::FromProto(table.GetPathId());
            auto columnTags = TVector<ui32>(
                table.GetColumnTags().begin(),table.GetColumnTags().end());
            const TString columnTagsStr = JoinVectorIntoString(columnTags, ",");
            const auto status = TForceTraversalTable::EStatus::None;
            const TString path = table.GetPath();

            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Create new force traversal table"
                << ", OperationId: " << operationId.Quote()
                << ", PathId: " << pathId
                << ", ColumnTags: " << columnTagsStr);

            TForceTraversalTable operationTable {
                .PathId = pathId,
                .ColumnTags = std::move(columnTags),
                .Path = path,
                .Status = status
            };
            operation.Tables.emplace_back(operationTable);

            db.Table<Schema::ForceTraversalTables>().Key(operationId, pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ForceTraversalTables::OperationId>(operationId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::OwnerId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::LocalPathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::ColumnTags>(columnTagsStr),
                NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)status),
                NIceDb::TUpdate<Schema::ForceTraversalTables::Path>(path)
            );
        }

        Self->ForceTraversals.emplace_back(operation);
        Self->RecalcForceTraversalsInflightSizeCounter();

        db.Table<Schema::ForceTraversalOperations>().Key(operationId).Update(
            NIceDb::TUpdate<Schema::ForceTraversalOperations::OperationId>(operationId),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::Types>(types),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::CreatedAt>(createdAt.GetValue()),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::DatabaseName>(databaseName),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::ReplyToActorId>(ReplyToActorId)
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Complete");

        if (TerminalReplay && ReplyToActorId) {
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(Record().GetOperationId());
            response->Record.SetStatus(*TerminalReplay);
            for (const auto& issue : TerminalReplayIssues) {
                NYql::IssueToMessage(issue, response->Record.AddIssues());
            }
            ctx.Send(ReplyToActorId, response.release());
            return;
        }

        ctx.Send(Self->SelfId(), new TEvPrivate::TEvScheduleTraversal());
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    Execute(new TTxAnalyze(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
