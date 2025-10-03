#pragma once
#include "defs.h"

#include "flat_scan_iface.h"
#include "flat_dbase_scheme.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NTabletFlatExecutor::NBackup {

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

    EvWriteSnapshot = EvBegin + 1536,
    EvWriteSnapshotAck,
    EvSnapshotCompleted,

    EvWriteChangelog,
    EvChangelogFailed,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR));

struct TEvSnapshotCompleted : public TEventLocal<TEvSnapshotCompleted, EvSnapshotCompleted> {
    TEvSnapshotCompleted(bool success, const TString& error = "")
        : Success(success)
        , Error(error)
    {}

    bool Success;
    TString Error;
};

enum class EScanStatus {
    InProgress = 0,
    Done = 1,
    Lost = 2,
    Term = 3,
    StorageError = 4,
    Exception = 5,
};

struct TEvWriteSnapshot : public TEventLocal<TEvWriteSnapshot, EvWriteSnapshot> {
    TEvWriteSnapshot(ui32 tableId, TBuffer&& snapshotData, EScanStatus scanStatus)
        : TableId(tableId)
        , SnapshotData(std::move(snapshotData))
        , ScanStatus(scanStatus)
    {}

    ui32 TableId;
    TBuffer SnapshotData;
    EScanStatus ScanStatus;
};

struct TEvWriteSnapshotAck : public TEventLocal<TEvWriteSnapshotAck, EvWriteSnapshotAck> {};

struct TEvWriteChangelog : public TEventLocal<TEvWriteChangelog, EvWriteChangelog> {
    TEvWriteChangelog(ui32 step, const TString& logBody, const TVector<TEvTablet::TLogEntryReference>& references)
        : Step(step)
        , EmbeddedLogBody(logBody)
        , References(references)
    {}

    ui32 Step;
    TString EmbeddedLogBody;
    TVector<TEvTablet::TLogEntryReference> References;
};

struct TEvChangelogFailed : public TEventLocal<TEvChangelogFailed, EvChangelogFailed> {
    TEvChangelogFailed(const TString& error)
        : Error(error)
    {}

    TString Error;
};

IActor* CreateSnapshotWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                             const THashMap<ui32, NTable::TScheme::TTableInfo>& tables,
                             TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation,
                             TAutoPtr<NTable::TSchemeChanges> schema);
NTable::IScan* CreateSnapshotScan(TActorId snapshotWriter, ui32 tableId, const THashMap<ui32, NTable::TColumn>& columns);

IActor* CreateChangelogWriter(TActorId owner, const NKikimrConfig::TSystemTabletBackupConfig& config,
                              TTabletTypes::EType tabletType, ui64 tabletId, ui32 generation,
                              const NTable::TScheme& schema);
} // NKikimr::NTabletFlatExecutor::NBackup
