#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>

namespace NKikimr {

    namespace NSyncer {
        class TSyncerJobTask;
    }
    class TSyncerContext;

    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerJobDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerGenerationCutApplied
        : TEventLocal<TEvSyncerGenerationCutApplied, TEvBlobStorage::EvSyncerGenerationCutApplied>
    {
        TVDiskID Source;
        ui64 Incarnation;

        TEvSyncerGenerationCutApplied(const TVDiskID& source, ui64 incarnation)
            : Source(source)
            , Incarnation(incarnation)
        {}
    };

    struct TEvSyncerJobDone :
            public TEventLocal<TEvSyncerJobDone, TEvBlobStorage::EvSyncJobDone>
    {
        std::unique_ptr<NSyncer::TSyncerJobTask> Task;

        TEvSyncerJobDone(std::unique_ptr<NSyncer::TSyncerJobTask> task)
            : Task(std::move(task))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerJob CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncerJob(const TIntrusivePtr<TSyncerContext> &sc,
                            std::unique_ptr<NSyncer::TSyncerJobTask> task,
                            const TActorId &notifyId);

} // NKikimr
