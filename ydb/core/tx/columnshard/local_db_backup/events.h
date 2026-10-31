#pragma once

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tx/columnshard/columnshard.h>

#include <ydb/library/actors/core/event_local.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

struct TEvStartBackup: public NActors::TEventLocal<TEvStartBackup, TEvColumnShard::EvLocalDbBackupStart> {
    NActors::TActorId Writer;

    explicit TEvStartBackup(const NActors::TActorId& writer)
        : Writer(writer)
    {
    }
};

struct TEvBackupStarted: public NActors::TEventLocal<TEvBackupStarted, TEvColumnShard::EvLocalDbBackupStarted> {
    ui32 Generation = 0;
    ui32 Step = 0;
    TAutoPtr<NTable::TSchemeChanges> Schema;
    TMap<ui32, TString> Tables;   // tableId -> table name
    TString BackupPath;
};

struct TEvCancelScans: public NActors::TEventLocal<TEvCancelScans, TEvColumnShard::EvLocalDbBackupCancelScans> {};

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
