#include "blobstorage_synclog_private_events.h"
#include "blobstorage_synclogdata.h"

namespace NKikimr {
    namespace NSyncLog {

        TEvSyncLogSnapshotResult::TEvSyncLogSnapshotResult(
                const TIntrusivePtr<TSyncLogSnapshot> &ptr,
                const TString &sublogContent)
            : SnapshotPtr(ptr)
            , SublogContent(sublogContent)
        {}

        TEvSyncLogSnapshotResult::~TEvSyncLogSnapshotResult() = default;

        TEvPhantomFlagStorageWriteItems::TEvPhantomFlagStorageWriteItems(
                std::vector<TPhantomFlagStorageItem>&& items)
            : Items(std::move(items))
        {}

        TEvPhantomFlagStorageCommitData::TEvPhantomFlagStorageCommitData(
                const std::optional<TPhantomFlagStorageData>& data)
            : Data(data)
        {}

    } // NSyncLog
} // NKikimr

