#pragma once

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tx/columnshard/columnshard.h>

#include <ydb/library/actors/core/event_local.h>

#include <util/generic/map.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NColumnShard::NLocalDbBackup {

using TGeneration = ui32;
using TStep = ui32;
using TTableId = ui32;

struct TEvStartBackup: public NActors::TEventLocal<TEvStartBackup, TEvColumnShard::EvLocalDbBackupStart> {
    NActors::TActorId Writer;

    explicit TEvStartBackup(const NActors::TActorId& writer)
        : Writer(writer)
    {
    }
};

struct TEvBackupStarted: public NActors::TEventLocal<TEvBackupStarted, TEvColumnShard::EvLocalDbBackupStarted> {
    TGeneration Generation = 0;
    TStep Step = 0;
    std::unique_ptr<NTable::TSchemeChanges> Schema;
    TMap<TTableId, TString> Tables;
    TString BackupPath;
};

struct TEvCancelBackup: public NActors::TEventLocal<TEvCancelBackup, TEvColumnShard::EvLocalDbBackupCancel> {};

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
