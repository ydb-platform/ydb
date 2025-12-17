#pragma once
#include <ydb/core/ymq/error/error.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/ymq/base/counters.h>

namespace NKikimr::NSQS {

size_t ErrorsCount(const NKikimrClient::TSqsResponse& response, TAPIStatusesCounters* counters);

} // namespace NKikimr::NSQS
