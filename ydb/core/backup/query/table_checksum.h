#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NBackup {

struct TEvTableChecksum {
    // Reserve a separate range from the backup collection service events
    enum EEvents {
        EvStart = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE) + 1024,
        EvResult,
        EvSnapshotAcquired,
        EvCheckRetentionRequest,
        EvCheckRetentionResponse,
        EvRetentionUnsupported,
    };

    struct TEvStart : NActors::TEventLocal<TEvStart, EvStart> {};

    // Only a successful capability probe of an older SchemeShard may skip the checksum
    struct TEvRetentionUnsupported : NActors::TEventLocal<TEvRetentionUnsupported, EvRetentionUnsupported> {};

    struct TEvCheckRetentionRequest : TEventPB<TEvCheckRetentionRequest,
                                          NKikimrScheme::TCheckTableChecksumRetentionRequest, EvCheckRetentionRequest> {
    };

    struct TEvCheckRetentionResponse
        : TEventPB<TEvCheckRetentionResponse, NKikimrScheme::TCheckTableChecksumRetentionResponse,
              EvCheckRetentionResponse> {};

    // Import owners persist the checkpoint and acknowledge it with TEvStart before the SELECT
    struct TEvSnapshotAcquired : NActors::TEventLocal<TEvSnapshotAcquired, EvSnapshotAcquired> {
        ui64 OperationId;
        ui32 ItemIdx;
        TRowVersion Snapshot;

        TEvSnapshotAcquired(ui64 operationId, ui32 itemIdx, TRowVersion snapshot)
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

        TEvResult(ui64 operationId, ui32 itemIdx, Ydb::StatusIds::StatusCode status, const TTableChecksum& checksum,
            const TString& error)
            : OperationId(operationId)
            , ItemIdx(itemIdx)
            , Status(status)
            , Checksum(checksum)
            , Error(error)
        {}
    };
};

// Starts only after TEvStart, allowing the owner to commit its state first
// An explicit export snapshot uses retention held by the table's SchemeShard
NActors::IActor* CreateTableChecksumQuery(NActors::TActorId replyTo, ui64 operationId, ui32 itemIdx,
    const TString& database, const TString& tablePath, bool importing, const TTableId& tableId = {},
    TMaybe<TRowVersion> snapshot = Nothing());

} // namespace NKikimr::NBackup
