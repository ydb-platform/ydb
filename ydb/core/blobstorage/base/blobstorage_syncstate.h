#pragma once
#include <util/stream/fwd.h>

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>



namespace NKikimrBlobStorage {
    class TSyncState;
}

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // SYNC STATE
    ////////////////////////////////////////////////////////////////////////////
    struct TSyncState {
        TVDiskIncarnationGuid Guid;
        ui64 SyncedLsn;

        TSyncState()
            : Guid(0)
            , SyncedLsn(0)
        {}

        TSyncState(const TVDiskIncarnationGuid &guid, ui64 syncedLsn)
            : Guid(guid)
            , SyncedLsn(syncedLsn)
        {}

        bool operator ==(const TSyncState &x) const {
            return Guid == x.Guid && SyncedLsn == x.SyncedLsn;
        }

        bool operator !=(const TSyncState &x) const {
            return !operator ==(x);
        }

        void Serialize(IOutputStream &s) const;

        bool Deserialize(IInputStream &s);

        void Output(IOutputStream &str) const;

        TString ToString() const;
    };

    TSyncState SyncStateFromSyncState(const NKikimrBlobStorage::TSyncState &x);

    void SyncStateFromSyncState(const TSyncState &state, NKikimrBlobStorage::TSyncState *proto);

} // NKikimr

