#pragma once
#include "blob.h"
#include "defs.h"

#include "common/snapshot.h"
#include "public/events.h"

#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/data_events/common/modification_type.h>
#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/tx/tx.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {

namespace NOlap {
class TPKRangesFilter;
}

namespace TEvColumnShard {
struct TEvInternalScan: public TEventLocal<TEvInternalScan, EvInternalScan> {
private:
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);
    YDB_READONLY(NOlap::TSnapshot, Snapshot, NOlap::TSnapshot::Zero());
    YDB_READONLY_DEF(std::optional<ui64>, LockId);
    YDB_READONLY_DEF(bool, ReadOnlyConflicts);
    YDB_ACCESSOR(bool, Reverse, false);
    YDB_ACCESSOR(ui32, ItemsLimit, 0);
    YDB_READONLY_DEF(std::vector<ui32>, ColumnIds);
    std::set<ui32> ColumnIdsSet;

public:
    std::optional<ui64> SchemaVersion;
    TString TaskIdentifier;
    std::shared_ptr<NOlap::TPKRangesFilter> RangesFilter;

public:
    void AddColumn(const ui32 id) {
        AFL_VERIFY(ColumnIdsSet.emplace(id).second);
        ColumnIds.emplace_back(id);
    }

    TEvInternalScan(const NColumnShard::TUnifiedPathId pathId, const NOlap::TSnapshot& snapshot, const std::optional<ui64> lockId,
        const bool readOnlyConflicts)
        : PathId(pathId)
        , Snapshot(snapshot)
        , LockId(lockId)
        , ReadOnlyConflicts(readOnlyConflicts)
    {
        AFL_VERIFY(Snapshot.Valid());
    }

    TString ToString() const {
        auto columns = TStringBuilder() << "[";
        for (size_t i = 0; i != ColumnIds.size(); ++i) {
            columns << ColumnIds[i];
            if (i != ColumnIds.size() - 1) {
                columns << ", ";
            }
        }
        columns << "]";
        return TStringBuilder() << "TEvInternalScan { PathId: " << PathId << ", Snapshot: " << Snapshot << ", LockId: " << LockId
                                << ", Reverse: " << Reverse << ", ItemsLimit: " << ItemsLimit << ", ColumnIds: " << columns << " }";
    }
};
};   // namespace TEvColumnShard

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info);

}   // namespace NKikimr
