#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTable : public TTxBase {
    const NKikimrStat::TEvAnalyze& Record;
    TActorId ReplyToActorId;    

    TTxAnalyzeTable(TSelf* self, const NKikimrStat::TEvAnalyze& record, TActorId replyToActorId)
        : TTxBase(self)
        , Record(record)
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute. ReplyToActorId " << ReplyToActorId << " , Record " << Record);

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const TString operationId = Record.GetOperationId();
        const TString types = JoinVectorIntoString(TVector<ui32>(Record.GetTypes().begin(), Record.GetTypes().end()), ",");
        
        for (const auto& table : Record.GetTables()) {
            const TPathId pathId = PathIdFromPathId(table.GetPathId());
            const TString columnTags = JoinVectorIntoString(TVector<ui32>{table.GetColumnTags().begin(),table.GetColumnTags().end()},",");

            // check existing force traversal with the same cookie and path
            auto forceTraversal = std::find_if(Self->ForceTraversals.begin(), Self->ForceTraversals.end(), 
                [&pathId, &operationId](const TForceTraversal& elem) { 
                    return elem.PathId == pathId 
                        && elem.OperationId == operationId;});

            // update existing force traversal
            if (forceTraversal != Self->ForceTraversals.end()) {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute. Update existing force traversal. PathId " << pathId << " , ReplyToActorId " << ReplyToActorId);
                forceTraversal->ReplyToActorId = ReplyToActorId;
                return true;
            }

            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute. Create new force traversal operation for pathId " << pathId);

            // create new force trasersal
            TForceTraversal operation {
                .OperationId = operationId,
                .PathId = pathId,
                .ColumnTags = columnTags,
                .Types = types,
                .ReplyToActorId = ReplyToActorId
            };
            Self->ForceTraversals.emplace_back(operation);
/*
            db.Table<Schema::ForceTraversals>().Key(Self->NextForceTraversalOperationId, pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ForceTraversals::OperationId>(Self->NextForceTraversalOperationId),
                NIceDb::TUpdate<Schema::ForceTraversals::OwnerId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::ForceTraversals::LocalPathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::ForceTraversals::Cookie>(cookie),
                NIceDb::TUpdate<Schema::ForceTraversals::ColumnTags>(columnTags),
                NIceDb::TUpdate<Schema::ForceTraversals::Types>(types)
            );
*/
        }

        return true;
    }

    void Complete(const TActorContext& /*ctx*/) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    Execute(new TTxAnalyzeTable(this, ev->Get()->Record, ev->Sender), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
