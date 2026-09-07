#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NDataShard {

void AddReadTraceAttributes(NWilson::TSpan& span, ui64 shardId, ui32 nodeId, ui64 readId);
void EndReadTrace(NWilson::TSpan& span, ui64 rows);
void FailReadTrace(NWilson::TSpan& span, ui64 rows, Ydb::StatusIds::StatusCode status, const TString& message);

}
