#include "kqp_worker_common.h"

namespace NKikimr::NKqp {

void SlowLogQuery(const TActorContext &ctx, const NYql::TKikimrConfiguration* config, const TKqpRequestInfo& requestInfo,
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
