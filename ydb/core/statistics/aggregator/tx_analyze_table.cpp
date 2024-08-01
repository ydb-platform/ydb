#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTable : public TTxBase {
    ui64 Cookie;
    TPathId PathId;
    TActorId ReplyToActorId;

    TTxAnalyzeTable(TSelf* self, ui64 cookie, const TPathId& pathId, TActorId replyToActorId)
        : TTxBase(self)
        , Cookie(cookie)
        , PathId(pathId)
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute");

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        // drop request with the same cookie and path
        auto it = std::find_if(Self->ForceTraversals.begin(), Self->ForceTraversals.end(), 
            [this](const TForceTraversal& elem) { return elem.PathId == PathId && elem.Cookie == Cookie;});
        if (it != Self->ForceTraversals.end()) {
            return true;
        }

        TAnalyzeResponseToActor analyzeResponse {
            .Cookie = Cookie,
            .ActorId = ReplyToActorId
        };
        
        // subscribe to request with the same path
        it = std::find_if(Self->ForceTraversals.begin(), Self->ForceTraversals.end(), 
            [this](const TForceTraversal& elem) { return elem.PathId == PathId;});
        if (it != Self->ForceTraversals.end()) {
            it->AnalyzeResponseToActors.emplace_back(analyzeResponse);
            return true;
        }

        // create new force trasersal
        TForceTraversal operation {
            .Cookie = Cookie,
            .PathId = PathId,
            .AnalyzeResponseToActors = {analyzeResponse}
        };
        Self->ForceTraversals.emplace_back(operation);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::ForceTraversals>().Key(Cookie, PathId.OwnerId, PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ForceTraversals::OwnerId>(Cookie),
            NIceDb::TUpdate<Schema::ForceTraversals::OwnerId>(PathId.OwnerId),
            NIceDb::TUpdate<Schema::ForceTraversals::LocalPathId>(PathId.LocalPathId));

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    // TODO: replace by queue
    for (const auto& table : record.GetTables()) {
        Execute(new TTxAnalyzeTable(this, record.GetCookie(), PathIdFromPathId(table.GetPathId()), ev->Sender), TActivationContext::AsActorContext());
    }

}

} // NKikimr::NStat
