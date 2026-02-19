#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyze : public TTxBase {
    TEvStatistics::TEvAnalyze::TPtr Event;
    TActorId ReplyToActorId;

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

        // check existing force traversal with the same OperationId
        const auto existingOperation = Self->ForceTraversalOperation(operationId);

        // update existing force traversal
        if (existingOperation) {
            if (existingOperation->ReplyToActorId == Event->Sender) {
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

            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Create new force traversal table"
                << ", OperationId: " << operationId.Quote()
                << ", PathId: " << pathId
                << ", ColumnTags: " << columnTagsStr);

            // create new force traversal
            TForceTraversalTable operationTable {
                .PathId = pathId,
                .ColumnTags = std::move(columnTags),
                .Status = status
            };
            operation.Tables.emplace_back(operationTable);

            db.Table<Schema::ForceTraversalTables>().Key(operationId, pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ForceTraversalTables::OperationId>(operationId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::OwnerId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::LocalPathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::ColumnTags>(columnTagsStr),
                NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)status)
            );
        }

        Self->ForceTraversals.emplace_back(operation);
        Self->TabletCounters->Simple()[COUNTER_FORCE_TRAVERSALS_INFLIGHT_SIZE].Set(Self->ForceTraversals.size());

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

        ctx.Send(Self->SelfId(), new TEvPrivate::TEvScheduleTraversal());
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    Execute(new TTxAnalyze(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
