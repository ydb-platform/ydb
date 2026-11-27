#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/backup/common/table_checksum.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <atomic>
#include <memory>

namespace NKikimr::NBackup {

struct TEvTableChecksum {
    // Reserve a separate range from the backup collection service events
    enum EEvents {
        EvStart = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE) + 1024,
        EvResult,
        EvPrepareChecksum,
        EvRunItem,
        EvFinishItem,
        EvStopOperation,
        EvInternalWakeup,
        EvSnapshotRequestTimeout,
        EvInternalSessionCreated,
        EvInternalQueryCompleted,
        EvImportChecksumIdle,
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

    struct TOwner {
        std::atomic<bool> Alive = true;
    };

    using TOwnerPtr = std::shared_ptr<TOwner>;

    struct TItem {
        ui64 OperationId;
        ui32 ItemIdx;
        TString Database;
        TString TablePath;
        bool Importing;
        TTableId TableId;
        TRowVersion Snapshot;
        TString Algorithm;
    };

    struct TEvPrepareChecksum : NActors::TEventLocal<TEvPrepareChecksum, EvPrepareChecksum> {
        TOwnerPtr Owner;
        TItem Item;

        TEvPrepareChecksum(TOwnerPtr owner, TItem item)
            : Owner(std::move(owner))
            , Item(std::move(item))
        {}
    };

    struct TEvRunItem : NActors::TEventLocal<TEvRunItem, EvRunItem> {
        TOwnerPtr Owner;
        TItem Item;

        TEvRunItem(TOwnerPtr owner, TItem item)
            : Owner(std::move(owner))
            , Item(std::move(item))
        {}
    };

    struct TEvFinishItem : NActors::TEventLocal<TEvFinishItem, EvFinishItem> {
        ui64 OperationId;
        ui32 ItemIdx;

        TEvFinishItem(ui64 operationId, ui32 itemIdx)
            : OperationId(operationId)
            , ItemIdx(itemIdx)
        {}
    };

    struct TEvStopOperation : NActors::TEventLocal<TEvStopOperation, EvStopOperation> {
        ui64 OperationId;

        explicit TEvStopOperation(ui64 operationId)
            : OperationId(operationId)
        {}
    };

    // Reports executor idleness using the latest nonzero stop cookie
    struct TEvImportChecksumIdle : NActors::TEventLocal<TEvImportChecksumIdle, EvImportChecksumIdle> {};
};

} // namespace NKikimr::NBackup
