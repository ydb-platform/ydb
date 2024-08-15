#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTableRequest : public TTxBase {
    std::vector<std::unique_ptr<IEventBase>> Events;
    
    TTxAnalyzeTableRequest(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE_REQUEST; }

    static std::unique_ptr<TEvStatistics::TEvAnalyzeTable> MakeRequest(const TString& operationId, const TForceTraversalTable& operationTable) {
        auto request = std::make_unique<TEvStatistics::TEvAnalyzeTable>();
        auto& record = request->Record;
        record.SetOperationId(operationId);
        auto& table = *record.MutableTable();
        PathIdFromPathId(operationTable.PathId, table.MutablePathId());
        TVector<ui32> columnTags = Scan<ui32>(SplitString(operationTable.ColumnTags, ","));
        table.MutableColumnTags()->Add(columnTags.begin(), columnTags.end());
        return request;
    }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeTableRequest::Execute");

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            for (TForceTraversalTable& operationTable : operation.Tables) {
                if (operationTable.Status == TForceTraversalTable::EStatus::AnalyzeStarted) {
                    while(!operationTable.ShardIdsToAnalyze.empty()) {
                        auto shardId = operationTable.ShardIdsToAnalyze.begin();
                        
                        auto request = MakeRequest(operation.OperationId, operationTable);
                        Events.push_back(std::make_unique<TEvPipeCache::TEvForward>(request.release(), *shardId, true));
                        
                        operationTable.ShardIdsToAnalyze.erase(shardId);

                        if (Events.size() == SendAnalyzeCount)
                            return true;
                    }
                }
            }
        }        

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Events.size()) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTableRequest::Complete. Send " << Events.size() << " events.");
        }
        else {
            SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeTableRequest::Complete.");
        }

        for (auto& ev : Events) {
            Self->Send(MakePipePerNodeCacheID(false), ev.release() );
        }

        if (Events.size() == SendAnalyzeCount) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvSendAnalyze());
        } else {
            ctx.Schedule(SendAnalyzePeriod, new TEvPrivate::TEvSendAnalyze());
        }
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvSendAnalyze::TPtr&) {
    Execute(new TTxAnalyzeTableRequest(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
