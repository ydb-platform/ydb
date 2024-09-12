#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace NKikimr::NDataShard {

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        TPathId tablePathId,
        const TPathId& targetPathId,
        ui64 txId);

} // namespace NKikimr::NDataShard
