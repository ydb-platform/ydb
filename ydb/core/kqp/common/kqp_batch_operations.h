#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/fwd.h>

namespace NKikimr::NKqp::NBatchOperations {

bool IsIndexSupported(NYql::TIndexDescription::EType type, bool enabledIndexStreamWrite = false);

TSerializedTableRange MakePartitionRange(TMaybe<TKeyDesc::TPartitionRangeInfo> begin, TMaybe<TKeyDesc::TPartitionRangeInfo> end, size_t keySize);

struct TSettings {
    ui64 MaxBatchSize = 10000;
    ui64 MinBatchSize = 1;
    ui64 MaxRetryDelayMs = 30000;
    ui64 StartRetryDelayMs = 50;
    ui64 PartitionExecutionLimit = 10;
};

TSettings ImportSettingsFromProto(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings);

} // namespace NKikimr::NKqp::NBatchOperations
