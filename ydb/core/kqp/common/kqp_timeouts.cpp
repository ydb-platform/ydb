#include "kqp_timeouts.h"

namespace NKikimr::NKqp {


namespace {

ui64 GetDefaultQueryTimeoutMs(NKikimrKqp::EQueryType queryType,
                              const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
                              const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
    const auto& queryLimits = tableServiceConfig.GetQueryLimits();

    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            return queryLimits.GetSchemeQueryTimeoutMs();

        case NKikimrKqp::QUERY_TYPE_SQL_DML:
        case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
        case NKikimrKqp::QUERY_TYPE_AST_DML:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            return queryLimits.GetDataQueryTimeoutMs();

        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return queryServiceConfig.GetScriptOperationTimeoutDefaultSeconds() 
                   ? queryServiceConfig.GetScriptOperationTimeoutDefaultSeconds()
                   : SCRIPT_TIMEOUT_LIMIT.MilliSeconds();

        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            return queryLimits.GetScanQueryTimeoutMs();

        default:
            return 600000;
    }
}

}

TDuration GetQueryTimeout(NKikimrKqp::EQueryType queryType,
                          ui64 timeoutMs,
                          const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
                          const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
    ui64 defaultTimeoutMs = GetDefaultQueryTimeoutMs(queryType, tableServiceConfig, queryServiceConfig);


    return timeoutMs
        ? TDuration::MilliSeconds(Min(defaultTimeoutMs, timeoutMs))
        : TDuration::MilliSeconds(defaultTimeoutMs);
}

}
