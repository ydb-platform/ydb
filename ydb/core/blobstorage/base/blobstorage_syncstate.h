#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>

#include <util/stream/input.h>
#include <util/string/printf.h>

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

        void Serialize(IOutputStream &s) const {
            s.Write(&Guid, sizeof(Guid));
            s.Write(&SyncedLsn, sizeof(SyncedLsn));
        }

        bool Deserialize(IInputStream &s) {
            if (s.Load(&Guid, sizeof(Guid)) != sizeof(Guid))
                return false;
            if (s.Load(&SyncedLsn, sizeof(SyncedLsn)) != sizeof(SyncedLsn))
                return false;
            return true;
        }

        void Output(IOutputStream &str) const {
            str << "[" << Guid << " " << SyncedLsn << "]";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }
    };

    inline TSyncState SyncStateFromSyncState(const NKikimrBlobStorage::TSyncState &x) {
        return TSyncState(x.GetGuid(), x.GetSyncedLsn());
    }

    inline void SyncStateFromSyncState(const TSyncState &state, NKikimrBlobStorage::TSyncState *proto) {
        proto->SetGuid(state.Guid);
        proto->SetSyncedLsn(state.SyncedLsn);
    }

} // NKikimr

