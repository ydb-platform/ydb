#pragma once
#include <ydb/library/conclusion/generic/result.h>
#include <ydb/library/conclusion/generic/status.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {

using TYdbConclusionStatus = TConclusionStatusImpl<Ydb::StatusIds::StatusCode, Ydb::StatusIds::SUCCESS, Ydb::StatusIds::INTERNAL_ERROR>;

template <typename TResult>
using TYdbConclusion = TConclusionImpl<TYdbConclusionStatus, TResult>;

}   // namespace NKikimr
