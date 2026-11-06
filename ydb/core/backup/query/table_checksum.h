#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NBackup {

struct TEvTableChecksum {
    // Reserve a separate range from the backup collection service events
    enum EEvents {
        EvStart = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE) + 1024,
        EvResult,
    };

    struct TEvStart : NActors::TEventLocal<TEvStart, EvStart> {};

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
NActors::IActor* CreateTableChecksumQuery(NActors::TActorId replyTo, ui64 operationId, ui32 itemIdx,
    const TString& database, const TString& tablePath, bool importing);

} // namespace NKikimr::NBackup
