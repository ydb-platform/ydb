#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvVDiskGuidWritten
    ////////////////////////////////////////////////////////////////////////////
    struct TEvVDiskGuidWritten : public
        TEventLocal<TEvVDiskGuidWritten, TEvBlobStorage::EvVDiskGuidWritten>
    {
        // from protobuf
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;

        // Written to this vdisk
        const TVDiskID VDiskId;
        // Guid value
        const TVDiskEternalGuid Guid;
        // Our confidence about given Guid
        const ESyncState State;

        TEvVDiskGuidWritten(const TVDiskID &vdisk, TVDiskEternalGuid guid, ESyncState state)
            : VDiskId(vdisk)
            , Guid(guid)
            , State(state)
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
    // CreateProxyForWritingVDiskGuid
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    IActor *CreateProxyForWritingVDiskGuid(TIntrusivePtr<TVDiskContext> vctx,
                                           const TVDiskID &selfVDiskId,
                                           const TVDiskID &targetVDiskId,
                                           const TActorId &targetServiceId,
                                           const TActorId &notifyId,
                                           NKikimrBlobStorage::TSyncGuidInfo::EState state,
                                           TVDiskEternalGuid guid);

} // NKikimr
