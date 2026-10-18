#pragma once

#include <ydb/core/protos/tx_proxy.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;
class TQueryData;

struct TSnapshotTableRangeInfo {
    TString TablePath;
    TString FullTableReason;
    size_t KeyRangeReads = 0;
    size_t ComputedRangeReads = 0;
};

// Only uses literals and external parameters that are already available. An
// unresolved read of a table makes its snapshot cover the whole table.
TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable> BuildSnapshotTables(
    const TPreparedQueryHolder& preparedQuery, TQueryData& queryData,
    TVector<TSnapshotTableRangeInfo>* rangeInfo = nullptr);

} // namespace NKikimr::NKqp
