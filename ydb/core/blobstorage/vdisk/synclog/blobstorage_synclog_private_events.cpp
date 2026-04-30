#include "blobstorage_synclog_private_events.h"
#include "blobstorage_synclogdata.h"

namespace NKikimr {
    namespace NSyncLog {

        TEvSyncLogSnapshotResult::TEvSyncLogSnapshotResult(
                const TIntrusivePtr<TSyncLogSnapshot> &ptr,
                const TString &sublogContent,
                TSyncLogKeeperDebugInfo debugInfo)
            : SnapshotPtr(ptr)
            , SublogContent(sublogContent)
            , DebugInfo(std::move(debugInfo))
        {}

        TEvSyncLogSnapshotResult::~TEvSyncLogSnapshotResult() = default;

    } // NSyncLog
} // NKikimr
