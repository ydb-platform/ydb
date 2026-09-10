#pragma once

#include "kqp_query_tracing.h"

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NKqp {

namespace NPrivate {

template<class TStats>
void AddQueryExecutionAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const TStats& stats, ui64 requestUnits, Ydb::StatusIds::StatusCode status) {
    span.Attribute("db.query.summary", description.DisplayName);
    span.Attribute("db.operation.name", description.Operation);
    ui64 cpuUs = 0;
    ui64 rowsRead = 0;
    ui64 bytesRead = 0;
    ui64 rowsWritten = 0;
    ui64 waitUs = 0;
    ui64 spilledBytes = 0;
    double maxTaskSkew = 0;
    bool taskStatsIncomplete = false;
    for (const auto& execution : stats.GetExecutions()) {
        cpuUs += GetExecutionTraceCpuTimeUs(execution);
        NKqpProto::TKqpExecutionExtraStats extra;
        if (execution.GetExtra().UnpackTo(&extra)) {
            waitUs += extra.GetWaitTimeUs();
            spilledBytes += extra.GetSpilledBytes();
            maxTaskSkew = Max(maxTaskSkew, extra.GetMaxTaskSkew());
            taskStatsIncomplete |= extra.GetTaskStatsIncomplete();
        }
        for (const auto& table : execution.GetTables()) {
            rowsRead += table.GetReadRows();
            bytesRead += table.GetReadBytes();
            rowsWritten += table.GetWriteRows() + table.GetEraseRows();
        }
    }
    span.Attribute("ydb.cpu_us", static_cast<i64>(cpuUs));
    span.Attribute("ydb.session.cpu_us", static_cast<i64>(stats.GetWorkerCpuTimeUs()));
    span.Attribute("ydb.rows_read", static_cast<i64>(rowsRead));
    span.Attribute("ydb.bytes_read", static_cast<i64>(bytesRead));
    span.Attribute("ydb.rows_written", static_cast<i64>(rowsWritten));
    if (span.GetTraceId().GetVerbosity() >= TComponentTracingLevels::TQueryProcessor::Basic) {
        span.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
        span.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
        span.Attribute("ydb.max_task_skew", maxTaskSkew);
        span.Attribute("ydb.task_stats_incomplete", taskStatsIncomplete);
    }
    span.Attribute("ydb.consumed_ru", static_cast<i64>(requestUnits));
    span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
}

inline void AddQueryCompilationAttributes(NWilson::TSpan& span, bool fromCache, ui64 cpuUs, ui64 durationUs) {
    span.Attribute("ydb.compile.cpu_us", static_cast<i64>(cpuUs));
    span.Attribute("ydb.compile.cache_hit", fromCache);
    span.Attribute("ydb.compile.duration_us", static_cast<i64>(durationUs));
}

} // namespace NPrivate

template<class TStats>
void AddQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const TStats& stats, ui64 requestUnits, Ydb::StatusIds::StatusCode status) {
    if (!span) {
        return;
    }
    NPrivate::AddQueryExecutionAttributes(span, description, stats, requestUnits, status);
    if (stats.Compilation) {
        NPrivate::AddQueryCompilationAttributes(span, stats.Compilation->FromCache,
            stats.Compilation->CpuTimeUs, stats.Compilation->DurationUs);
    }
    span.Attribute("ydb.locks_broken_as_victim", static_cast<i64>(stats.LocksBrokenAsVictim));
    span.Attribute("ydb.locks_broken_as_breaker", static_cast<i64>(stats.LocksBrokenAsBreaker));
}

} // namespace NKikimr::NKqp
