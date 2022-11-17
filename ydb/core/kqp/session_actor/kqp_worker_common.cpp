#include "kqp_worker_common.h"

namespace NKikimr::NKqp {

using namespace NYql;

EKikimrStatsMode GetStatsModeInt(const NKikimrKqp::TQueryRequest& queryRequest) {
    switch (queryRequest.GetCollectStats()) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
            return EKikimrStatsMode::None;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            return EKikimrStatsMode::Basic;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return EKikimrStatsMode::Full;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            return EKikimrStatsMode::Profile;
        default:
            return EKikimrStatsMode::None;
    }
}

TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings) {
    const auto& queryLimitsProto = settings.Service.GetQueryLimits();
    const auto& phaseLimitsProto = queryLimitsProto.GetPhaseLimits();

    TKikimrQueryLimits queryLimits;
    auto& phaseLimits = queryLimits.PhaseLimits;
    phaseLimits.AffectedShardsLimit = phaseLimitsProto.GetAffectedShardsLimit();
    phaseLimits.ReadsetCountLimit = phaseLimitsProto.GetReadsetCountLimit();
    phaseLimits.ComputeNodeMemoryLimitBytes = phaseLimitsProto.GetComputeNodeMemoryLimitBytes();
    phaseLimits.TotalReadSizeLimitBytes = phaseLimitsProto.GetTotalReadSizeLimitBytes();

    return queryLimits;
}

void SlowLogQuery(const TActorContext &ctx, const TKikimrConfiguration* config, const TKqpRequestInfo& requestInfo,
    const TDuration& duration, Ydb::StatusIds::StatusCode status, const TString& userToken, ui64 parametersSize,
    NKikimrKqp::TEvQueryResponse *record, const std::function<TString()> extractQueryText)
{
    auto logSettings = ctx.LoggerSettings();
    if (!logSettings) {
        return;
    }

    ui32 thresholdMs = 0;
    NActors::NLog::EPriority priority;

    if (logSettings->Satisfies(NActors::NLog::PRI_TRACE, NKikimrServices::KQP_SLOW_LOG)) {
        priority = NActors::NLog::PRI_TRACE;
        thresholdMs = config->_KqpSlowLogTraceThresholdMs.Get().GetRef();
    } else if (logSettings->Satisfies(NActors::NLog::PRI_NOTICE, NKikimrServices::KQP_SLOW_LOG)) {
        priority = NActors::NLog::PRI_NOTICE;
        thresholdMs = config->_KqpSlowLogNoticeThresholdMs.Get().GetRef();
    } else if (logSettings->Satisfies(NActors::NLog::PRI_WARN, NKikimrServices::KQP_SLOW_LOG)) {
        priority = NActors::NLog::PRI_WARN;
        thresholdMs = config->_KqpSlowLogWarningThresholdMs.Get().GetRef();
    } else {
        return;
    }

    if (duration >= TDuration::MilliSeconds(thresholdMs)) {
        auto username = NACLib::TUserToken(userToken).GetUserSID();
        if (username.empty()) {
            username = "UNAUTHENTICATED";
        }

        Y_VERIFY_DEBUG(extractQueryText);
        auto queryText = extractQueryText();

        auto paramsText = TStringBuilder()
            << ToString(parametersSize)
            << 'b';

        ui64 resultsSize = 0;
        for (auto& result : record->GetResponse().GetResults()) {
            resultsSize += result.ByteSize();
        }

        LOG_LOG_S(ctx, priority, NKikimrServices::KQP_SLOW_LOG, requestInfo
            << "Slow query, duration: " << duration.ToString()
            << ", status: " << status
            << ", user: " << username
            << ", results: " << resultsSize << 'b'
            << ", text: \"" << EscapeC(queryText) << '"'
            << ", parameters: " << paramsText);
    }
}

} // namespace NKikimr::NKqp
