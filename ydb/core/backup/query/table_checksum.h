#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/backup/common/table_checksum.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NBackup {

TString GetTableChecksumSchemaHash(const NKikimrSchemeOp::TTableDescription& table);

struct TEvTableChecksum {
    // Reserve a separate range from the backup collection service events
    enum EEvents {
        EvStart = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE) + 1024,
        EvResult,
        EvRetentionUnsupported,
        EvSnapshotConfirmed,
        EvPrepareChecksum,
        EvFinish,
    };

    struct TEvStart : NActors::TEventLocal<TEvStart, EvStart> {};

    // Sent after the owner commits completion or cancellation; interruption uses TEvPoisonPill
    struct TEvFinish : NActors::TEventLocal<TEvFinish, EvFinish> {};

    // Skip checksum calculation when a DataShard does not support checksum snapshots
    struct TEvRetentionUnsupported : NActors::TEventLocal<TEvRetentionUnsupported, EvRetentionUnsupported> {};

    struct TEvSnapshotConfirmed : NActors::TEventLocal<TEvSnapshotConfirmed, EvSnapshotConfirmed> {
        Ydb::StatusIds::StatusCode Status;
        TRowVersion Snapshot;
        TString Error;
        bool Unsupported;

        TEvSnapshotConfirmed(
            Ydb::StatusIds::StatusCode status, TRowVersion snapshot, TString error, bool unsupported = false)
            : Status(status)
            , Snapshot(snapshot)
            , Error(std::move(error))
            , Unsupported(unsupported)
        {}
    };

    struct TEvPrepareChecksum : NActors::TEventLocal<TEvPrepareChecksum, EvPrepareChecksum> {
        ui64 OperationId;
        ui32 ItemIdx;
        TRowVersion Snapshot;

        TEvPrepareChecksum(ui64 operationId, ui32 itemIdx, TRowVersion snapshot)
            : OperationId(operationId)
            , ItemIdx(itemIdx)
            , Snapshot(snapshot)
        {}
    };

    struct TEvResult : NActors::TEventLocal<TEvResult, EvResult> {
        ui64 OperationId;
        ui32 ItemIdx;
        Ydb::StatusIds::StatusCode Status;
        TTableChecksum Checksum;
        TString Error;
        bool RetentionUnsupported;

        TEvResult(ui64 operationId, ui32 itemIdx, Ydb::StatusIds::StatusCode status, const TTableChecksum& checksum,
            const TString& error, bool retentionUnsupported = false)
            : OperationId(operationId)
            , ItemIdx(itemIdx)
            , Status(status)
            , Checksum(checksum)
            , Error(error)
            , RetentionUnsupported(retentionUnsupported)
        {}
    };
};

// Starts only after TEvStart, allowing the owner to commit its state first
// Copy and restore transactions retain the exact version on the table's DataShards
// schemeShardId is the current owner; tableId remains stable across SchemeShard migration
// Export also supplies copyTableId to retain the backup copy alongside the source
// Import supplies the recorded algorithm; export uses the query default
NActors::IActor* CreateTableChecksumQuery(NActors::TActorId replyTo, ui64 operationId, ui32 itemIdx,
    const TString& database, const TString& tablePath, bool importing, const TTableId& tableId, ui64 schemeShardId,
    TRowVersion snapshot, const TString& algorithm = {}, const TString& sourceSchemaHash = {},
    const TTableId& copyTableId = {});

} // namespace NKikimr::NBackup
