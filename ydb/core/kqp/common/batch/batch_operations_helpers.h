#pragma once

#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/fwd.h>

namespace NKikimr::NKqp::NBatchOperations {

const TString Header = "%kqp%batch_";
const TString IsInclusiveLeft = Header + "is_inclusive_left";
const TString IsInclusiveRight = Header + "is_inclusive_right";
const TString Begin = Header + "begin_"; // begin_N
const TString End = Header + "end_";     // end_N
const TString BeginPrefixSize = Begin + "prefix_size";
const TString EndPrefixSize = End + "prefix_size";

struct TBatchOperationPrunerSettings {
    TTableRange Range;
};

// TBatchOperationPrunerSettings MakeBatchOperationPrunerSettings(TMaybe<TKeyDesc::TPartitionRangeInfo> begin, TMaybe<TKeyDesc::TPartitionRangeInfo> end);

struct TBatchOperationSettings {
    ui64 MaxBatchSize = 10000;
    ui64 MinBatchSize = 1;
    ui64 MaxRetryDelayMs = 30000;
    ui64 StartRetryDelayMs = 50;
    ui64 PartitionExecutionLimit = 10;
};

TBatchOperationSettings SetBatchOperationSettings(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings);

} // namespace NKikimr::NKqp::NBatchOperations
