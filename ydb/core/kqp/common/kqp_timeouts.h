#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <util/datetime/base.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
    class TTableServiceConfig;
}

namespace NKikimr::NKqp {

constexpr TDuration SCRIPT_TIMEOUT_LIMIT = TDuration::Days(365);

TDuration GetQueryTimeout(NKikimrKqp::EQueryType queryType, ui64 timeoutMs, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, bool disableDefaultTimeout);

} // namespace NKikimr::NKqp
