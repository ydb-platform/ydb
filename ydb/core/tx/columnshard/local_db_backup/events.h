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
    // Writer this reply belongs to. A late reply of a failed run must not be applied to the next one.
    NActors::TActorId Writer;
    TGeneration Generation = 0;
    TStep Step = 0;
    std::unique_ptr<NTable::TSchemeChanges> Schema;
    TMap<TTableId, TString> Tables;
    TString BackupPath;
};

struct TEvCancelBackup: public NActors::TEventLocal<TEvCancelBackup, TEvColumnShard::EvLocalDbBackupCancel> {};

// Writer -> owner. The owner applies it to its own TProgress; the snapshot is not shared.
struct TEvBackupProgress: public NActors::TEventLocal<TEvBackupProgress, TEvColumnShard::EvLocalDbBackupProgress> {
    enum class EKind {
        Init,
        Stage,
        Bytes,
        Chunk,
        TableDone,
    };

    EKind Kind;
    TMap<TTableId, TString> Tables;
    TString Stage;
    TTableId TableId = 0;
    ui64 Bytes = 0;

    static std::unique_ptr<TEvBackupProgress> MakeInit(const TMap<TTableId, TString>& tables) {
        auto ev = std::make_unique<TEvBackupProgress>(EKind::Init);
        ev->Tables = tables;
        return ev;
    }

    static std::unique_ptr<TEvBackupProgress> MakeStage(TString stage) {
        auto ev = std::make_unique<TEvBackupProgress>(EKind::Stage);
        ev->Stage = std::move(stage);
        return ev;
    }

    static std::unique_ptr<TEvBackupProgress> MakeBytes(ui64 bytes) {
        auto ev = std::make_unique<TEvBackupProgress>(EKind::Bytes);
        ev->Bytes = bytes;
        return ev;
    }

    static std::unique_ptr<TEvBackupProgress> MakeChunk(TTableId tableId, ui64 bytes) {
        auto ev = std::make_unique<TEvBackupProgress>(EKind::Chunk);
        ev->TableId = tableId;
        ev->Bytes = bytes;
        return ev;
    }

    static std::unique_ptr<TEvBackupProgress> MakeTableDone(TTableId tableId) {
        auto ev = std::make_unique<TEvBackupProgress>(EKind::TableDone);
        ev->TableId = tableId;
        return ev;
    }

    explicit TEvBackupProgress(EKind kind)
        : Kind(kind)
    {
    }
};

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
