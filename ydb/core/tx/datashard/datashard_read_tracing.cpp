#include "datashard_read_tracing.h"

namespace NKikimr::NDataShard {

void AddReadTraceAttributes(NWilson::TSpan& span, ui64 shardId, ui32 nodeId, ui64 readId) {
    if (span) {
        span.Attribute("Shard", std::to_string(shardId));
        span.Attribute("ydb.shard_id", static_cast<i64>(shardId));
        span.Attribute("ydb.node_id", static_cast<i64>(nodeId));
        span.Attribute("ydb.actor.type", TString("DataShard"));
        span.Attribute("ydb.read_id", static_cast<i64>(readId));
    }
}

void EndReadTrace(NWilson::TSpan& span, ui64 rows) {
    if (span) {
        span.Attribute("ydb.rows", static_cast<i64>(rows));
        span.Attribute("ydb.finished", true);
        span.Attribute("ydb.status_code", TString("SUCCESS"));
        span.EndOk();
    }
}

void FailReadTrace(NWilson::TSpan& span, ui64 rows, Ydb::StatusIds::StatusCode status, const TString& message) {
    if (span) {
        span.Attribute("ydb.rows", static_cast<i64>(rows));
        span.Attribute("ydb.finished", false);
        span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
        span.EndError(message);
    }
}

}
