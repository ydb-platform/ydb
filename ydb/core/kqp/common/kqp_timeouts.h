#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <util/datetime/base.h>


namespace NKikimr::NKqp {

constexpr TDuration SCRIPT_TIMEOUT_LIMIT = TDuration::Days(365);

TDuration GetQueryTimeout(NKikimrKqp::EQueryType queryType,
                          ui64 timeoutMs,
                          const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
                          const NKikimrConfig::TQueryServiceConfig& queryServiceConfig);

}
