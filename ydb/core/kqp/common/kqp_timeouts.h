#pragma once

#include <ydb/core/protos/config.pb.h>
#include <util/datetime/base.h>


namespace NKikimr::NKqp {

TDuration GetQueryTimeout(NKikimrKqp::EQueryType queryType, ui64 timeoutMs, const NKikimrConfig::TTableServiceConfig& tableServiceConfig);

}
