#pragma once
#include "defs.h"

#include "flat_scan_iface.h"
#include "flat_dbase_scheme.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

    EvWriteSnapshot = EvBegin + 1536,
    EvCompleteSnapshot,
    EvSnapshotCompleted,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR), "");

struct TEvSnapshotCompleted : public TEventLocal<TEvSnapshotCompleted, EvSnapshotCompleted> {
    TEvSnapshotCompleted(bool success, const TString& error = "")
        : Success(success)
        , Error(error)
    {}

    bool Success;
    TString Error;
};

struct TEvCompleteSnapshot : public TEventLocal<TEvCompleteSnapshot, EvCompleteSnapshot> {
    TEvCompleteSnapshot(ui32 tableId, bool success, const TString& error = "")
        : TableId(tableId)
        , Success(success)
        , Error(error)
    {}

    ui32 TableId;
    bool Success;
    TString Error;
};

struct TEvWriteSnapshot : public TEventLocal<TEvWriteSnapshot, EvWriteSnapshot> {
    TEvWriteSnapshot(ui32 tableId, TBuffer&& snapshotData)
        : TableId(tableId)
        , SnapshotData(std::move(snapshotData))
    {}

    ui32 TableId;
    TBuffer SnapshotData;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, NTable::TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation);
NTable::IScan* CreateSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, NTable::TColumn>& columns);

} // NKikimr::NTabletFlatExecutor::NBackup
