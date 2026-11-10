#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

// Execution information supplied by either frontend, independently of its graph.
struct TQueryExecutionMetadata {
    TVector<NKqpProto::TKqpTableOp> TableOperations;
    TVector<bool> DiscardResults;
    bool HasUncommittedChangesRead = false;
};

} // namespace NKikimr::NKqp
