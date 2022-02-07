#pragma once

#include "defs.h"
#include "blobstorage_synclog_public_events.h"
#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // SYNC LOGIC
        // The algo below handles other VDisks reads, i.e. returns type of
        // activity to perform based on local SyncLog status and peer's state
        ////////////////////////////////////////////////////////////////////////////
        enum EReadWhatsNext {
            EWnError = 0,       // error, we can't get this in normal situation
            EWnFullSync = 1,    // run full sync
            EWnDiskSynced = 2,  // disk is already synced (no new updates)
            EWnMemRead = 3,     // handle request by reading from sync log cache
            EWnDiskRead = 4     // handle request by reading data from disk
        };

        struct TWhatsNextOutcome {
            EReadWhatsNext WhatsNext = EWnError;
            const TString Explanation;

            // good
            TWhatsNextOutcome(EReadWhatsNext wn)
                : WhatsNext(wn)
                , Explanation()
            {}

            // for all cases
            TWhatsNextOutcome(EReadWhatsNext wn, const TString &expl)
                : WhatsNext(wn)
                , Explanation(expl)
            {}

            // for error
            static TWhatsNextOutcome Error(int id) {
                TStringStream str;
                str << "Case# " << id;
                return TWhatsNextOutcome(EWnError, str.Str());
            }
        };

        const char *Name2Str(EReadWhatsNext w);
        TWhatsNextOutcome WhatsNext(ui64 syncedLsn, // lsn our peer VDisk synced to
                                    ui64 dbBirthLsn,// our db birth lsn
                                    const NSyncLog::TLogEssence *e,
                                    std::function<TString()> reportInternals);

        ////////////////////////////////////////////////////////////////////////////
        // CreateSyncLogReaderActor
        // Actor that process read request
        ////////////////////////////////////////////////////////////////////////////
        IActor* CreateSyncLogReaderActor(
            const TIntrusivePtr<TSyncLogCtx> &slCtx,
            TVDiskIncarnationGuid vdiskIncarnationGuid,
            TEvBlobStorage::TEvVSync::TPtr &ev,
            const TActorId &parentId,
            const TActorId &keeperId,
            const TVDiskID &selfVDiskId,
            const TVDiskID &sourceVDisk,
            ui64 dbBirthLsn,
            TInstant now);

    } // NSyncLog
} // NKikimr
