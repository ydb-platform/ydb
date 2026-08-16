#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <util/datetime/base.h>

namespace NKikimrConfig {

class TQueryServiceConfig;
class TTableServiceConfig;

} // namespace NKikimrConfig

namespace NKikimr::NKqp {

TDuration GetQueryTimeout(NKikimrKqp::EQueryType queryType, ui64 timeoutMs, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, bool disableDefaultTimeout);

} // namespace NKikimr::NKqp
