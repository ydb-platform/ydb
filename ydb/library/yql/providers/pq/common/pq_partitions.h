#pragma once

#include "pq_partition_key.h"

#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <util/generic/string.h>

#include <vector>

namespace NYql::NDq {

std::vector<NPq::NProto::TDqReadTaskParams> ExtractReadTaskParams(
    const THashMap<TString, TString>& taskParams, // partitions are here in dq
    const TVector<TString>& readRanges            // partitions are here in kqp
);

std::vector<ui64> GetPartitionsToRead(
    const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams
);

} // namespace NYql::NDq
