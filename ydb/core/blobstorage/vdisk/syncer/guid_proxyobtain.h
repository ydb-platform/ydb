#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvVDiskGuidObtained
    ////////////////////////////////////////////////////////////////////////////
    struct TEvVDiskGuidObtained : public
            TEventLocal<TEvVDiskGuidObtained, TEvBlobStorage::EvVDiskGuidObtained>
    {

        // from protobuf
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;

        // Guid obtained for this vdisk
        const TVDiskID VDiskId;
        // Guid value
        const TVDiskEternalGuid Guid;
        // Our confidence about given Guid
        const ESyncState State;
        const NKikimrBlobStorage::TSyncGuidHistory History;
        const NKikimrBlobStorage::TSyncGuidWitness RecoveredGuid;
        const bool HasHistory;

        TEvVDiskGuidObtained(const TVDiskID &vdisk, TVDiskEternalGuid guid, ESyncState state,
                const NKikimrBlobStorage::TEvVSyncGuidResult& record)
            : VDiskId(vdisk)
            , Guid(guid)
            , State(state)
            , History(record.GetGuidHistory())
            , RecoveredGuid(record.GetRecoveredGuid())
            , HasHistory(record.HasGuidHistory())
        {
            Y_ABORT_UNLESS(!(State == TSyncVal::Final && Guid == TVDiskEternalGuid()));
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void Output(IOutputStream &str) const {
            str << "{VDiskId# " << VDiskId
                << " Guid# " << Guid
                << " State# " << State << "}";
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateProxyForObtainingVDiskGuid
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    IActor* CreateProxyForObtainingVDiskGuid(TIntrusivePtr<TVDiskContext> vctx,
                                             const TVDiskID &selfVDiskId,
                                             const TVDiskID &targetVDiskId,
                                             const TActorId &targetServiceId,
                                             const TActorId &notifyId,
                                             bool readHistory = false);

} // NKikimr
