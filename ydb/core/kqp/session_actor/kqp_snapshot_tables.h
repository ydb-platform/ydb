#pragma once

#include <ydb/core/protos/tx_proxy.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NKqp {

class TPreparedQueryHolder;
class TQueryData;

// Tables for the persistent snapshot of a scan query. A table read only by
// key ranges that are known before execution gets one entry per range, so
// that TxProxy involves the shards being read only. Any other table gets an
// entry without a key range, that is a snapshot of the whole table.
TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable> BuildSnapshotTables(
    const TPreparedQueryHolder& preparedQuery, TQueryData& queryData);

// Number of entries with a key range, for logging.
size_t CountBoundedTables(const TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable>& tables);

} // namespace NKikimr::NKqp
