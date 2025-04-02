#pragma once

#include <ydb/core/protos/table_service_config.pb.h>
#include <util/generic/fwd.h>

namespace NKikimr::NKqp {

struct TBatchOperationSettings {
    ui64 MaxBatchSize = 10000;
    ui64 MinBatchSize = 1;
    ui64 MaxRetryDelay = 1000;
    ui64 MinRetryDelay = 50;
};

TBatchOperationSettings SetBatchOperationSettings(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings);

} // namespace NKikimr::NKqp
