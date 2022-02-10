#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>

namespace NKikimr {
    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////
        // EFirstRunStep
        // FirstRun phase has the following steps
        ////////////////////////////////////////////////////////////////////////
        enum class EFirstRunStep {
            STATE__Uninitialized,               // internal
            ACTION_GenerateGuid,                // *
            ACTION_WriteInProgressToQuorum,     // *
            STATE__WaitProgressWrittenToQuorum, // internal
            ACTION_WriteSelectedLocally,        // *
            STATE__WaitSelectedWrittenLocally,  // internal
            ACTION_WriteFinalToQuorum,          // *
            STATE__WaitFinalWrittenToQuorum,    // internal
            ACTION_WriteFinalLocally,           // *
            STATE__WaitFinalWrittenLocally,     // internal
            STATE__Terminated                   // internal
        };

        bool IsAction(EFirstRunStep s);
        const char *EFirstRunStepToStr(EFirstRunStep s);

    } // NSyncer

    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerGuidFirstRunDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerGuidFirstRunDone : public
        TEventLocal<TEvSyncerGuidFirstRunDone, TEvBlobStorage::EvSyncerGuidFirstRunDone>
    {
        const TVDiskEternalGuid Guid;

        TEvSyncerGuidFirstRunDone(TVDiskEternalGuid guid)
            : Guid(guid)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // VDISK FIRST RUN ACTOR
    // Implements algorithm of generating VDisk Guid and saving it to
    // quorum of VDisks
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    IActor *CreateVDiskGuidFirstRunActor(TIntrusivePtr<TVDiskContext> vctx,
                                         TIntrusivePtr<TBlobStorageGroupInfo> info,
                                         const TActorId &committerId,
                                         const TActorId &notifyId,
                                         NSyncer::EFirstRunStep startStep,
                                         TVDiskEternalGuid guid);

} // NKikimr
