#include "monitoring.h"

#include <ydb/core/ymq/actor/cfg.h>
#include <ydb/core/ymq/base/run_query.h>


namespace NKikimr::NSQS {
    
    constexpr TDuration RETRY_PERIOD_MIN = TDuration::Seconds(30);
    constexpr TDuration RETRY_PERIOD_MAX = TDuration::Minutes(5);
    

    TMonitoringActor::TMonitoringActor(TIntrusivePtr<TMonitoringCounters> counters)
        : Counters(counters)
        , RetryPeriod(RETRY_PERIOD_MIN)
    {}

    void TMonitoringActor::Bootstrap(const TActorContext& ctx) {
        Become(&TMonitoringActor::StateFunc);

        TString removedQueuesTable = Cfg().GetRoot() + "/.RemovedQueues";
        RemovedQueuesQuery = TStringBuilder() << R"__(
            --!syntax_v1
            SELECT RemoveTimestamp FROM `)__" << removedQueuesTable <<  R"__(` ORDER BY RemoveTimestamp LIMIT 1000;
        )__";

        RequestMetrics(TDuration::Zero(), ctx);
    }

    void TMonitoringActor::HandleError(const TString& error, const TActorContext& ctx) {
        auto runAfter = RetryPeriod;
        RetryPeriod = Min(RetryPeriod * 2, RETRY_PERIOD_MAX);
        LOG_ERROR_S(ctx, NKikimrServices::SQS, "[monitoring] Got an error : " << error);
        RequestMetrics(runAfter, ctx);
    }
    
    void TMonitoringActor::RequestMetrics(TDuration runAfter, const TActorContext& ctx) {
        RunYqlQuery(RemovedQueuesQuery, std::nullopt, true, runAfter, Cfg().GetRoot(), ctx);
    }
    
    void TMonitoringActor::HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            HandleError(record.DebugString(), ctx);
            return;
        }
        RetryPeriod = RETRY_PERIOD_MIN;
        auto& response = record.GetResponse();

        Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
        NYdb::TResultSetParser parser(response.GetYdbResults(0));
        TDuration removeQueuesDataLag;
        
        if (parser.RowsCount()) {
            parser.TryNextRow();
            TInstant minRemoveQueueTimestamp = TInstant::MilliSeconds(*parser.ColumnParser(0).GetOptionalUint64());
            removeQueuesDataLag = ctx.Now() - minRemoveQueueTimestamp;
        }
        
        LOG_DEBUG_S(ctx, NKikimrServices::SQS, "[monitoring] Report deletion queue data lag: " << removeQueuesDataLag << ", count: " << parser.RowsCount());
        *Counters->CleanupRemovedQueuesLagSec = removeQueuesDataLag.Seconds();
        *Counters->CleanupRemovedQueuesLagCount = parser.RowsCount();
        RequestMetrics(RetryPeriod, ctx);
    }

} // namespace NKikimr::NSQS
