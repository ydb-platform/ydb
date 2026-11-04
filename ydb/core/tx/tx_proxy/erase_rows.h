#pragma once

#include "defs.h"

#include <ydb/core/util/backoff.h>

namespace NKikimr {

class TSerializedCellVec;

namespace NTxProxy {

// Batch delete of rows by serialized key, analogous to CreateUploadRowsInternal.
using TEraseKeys = TVector<TSerializedCellVec>;

// Erases the given keys from `table`, resolving the table and its partitions
// through the scheme cache and fanning the keys out to the owning shards as
// TEvDataShard::TEvEraseRowsRequest. Replies to `sender` with a
// TEvTxUserProxy::TEvEraseRowsResponse.
//
// When writeToTableShadow is set the erases are applied to the shadow table of
// `table` (like UploadRows does for bulk upsert into index impl tables).
IActor* CreateEraseRowsInternal(const TActorId& sender,
                                const TString& database,
                                const TString& table,
                                std::shared_ptr<const TEraseKeys> keys,
                                bool writeToTableShadow = false,
                                bool writeToPrivateTable = false,
                                bool writeToIndexImplTable = false,
                                ui64 cookie = 0,
                                TBackoff backoff = TBackoff(0));
} // namespace NTxProxy
} // namespace NKikimr
