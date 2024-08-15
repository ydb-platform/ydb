#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTableResponse : public TTxBase {
    NKikimrStat::TEvAnalyzeTableResponse Record;
    
    TTxAnalyzeTableResponse(TSelf* self, NKikimrStat::TEvAnalyzeTableResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE_RESPONSE; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Execute");

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            for (TForceTraversalTable& operationTable : operation.Tables) {
                Y_UNUSED(operationTable);
            }
        }        

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Complete.");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeTableResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxAnalyzeTableResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
