#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyze : public TTxBase {
    const NKikimrStat::TEvAnalyze& Record;
    TActorId ReplyToActorId;    

    TTxAnalyze(TSelf* self, const NKikimrStat::TEvAnalyze& record, TActorId replyToActorId)
        : TTxBase(self)
        , Record(record)
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. ReplyToActorId " << ReplyToActorId << " , Record " << Record);

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const TString operationId = Record.GetOperationId();

        // check existing force traversal with the same OperationId
        const auto existingOperation = Self->ForceTraversalOperation(operationId);  

        // update existing force traversal
        if (existingOperation) {
            if (existingOperation->Tables.size() == Record.TablesSize()) {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Update existing force traversal. OperationId " << operationId << " , ReplyToActorId " << ReplyToActorId);
                existingOperation->ReplyToActorId = ReplyToActorId;
                return true;
            } else {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Delete broken force traversal. OperationId " << operationId << " , ReplyToActorId " << ReplyToActorId);
                Self->DeleteForceTraversalOperation(operationId, db);
            }
        }

        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Create new force traversal operation, OperationId=" << operationId);
        const TString types = JoinVectorIntoString(TVector<ui32>(Record.GetTypes().begin(), Record.GetTypes().end()), ",");

        // create new force trasersal
        auto createdAt = ctx.Now();
        TForceTraversalOperation operation {
            .OperationId = operationId,
            .Tables = {},
            .Types = types,
            .ReplyToActorId = ReplyToActorId,
            .CreatedAt = createdAt
        };

        for (const auto& table : Record.GetTables()) {
            const TPathId pathId = PathIdFromPathId(table.GetPathId());
            const TString columnTags = JoinVectorIntoString(TVector<ui32>{table.GetColumnTags().begin(),table.GetColumnTags().end()},",");
            const auto status = TForceTraversalTable::EStatus::None;

            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Execute. Create new force traversal table, OperationId=" << operationId << " , PathId " << pathId);

            // create new force traversal
            TForceTraversalTable operationTable {
                .PathId = pathId,
                .ColumnTags = columnTags,
                .Status = status
            };
            operation.Tables.emplace_back(operationTable);

            db.Table<Schema::ForceTraversalTables>().Key(operationId, pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ForceTraversalTables::OperationId>(operationId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::OwnerId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::LocalPathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::ForceTraversalTables::ColumnTags>(columnTags),
                NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)status)
            );
        }

        Self->ForceTraversals.emplace_back(operation);

        db.Table<Schema::ForceTraversalOperations>().Key(operationId).Update(
            NIceDb::TUpdate<Schema::ForceTraversalOperations::OperationId>(operationId),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::Types>(types),
            NIceDb::TUpdate<Schema::ForceTraversalOperations::CreatedAt>(createdAt.GetValue())
        );

        return true;
    }

    void Complete(const TActorContext& /*ctx*/) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyze::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    Execute(new TTxAnalyze(this, ev->Get()->Record, ev->Sender), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
