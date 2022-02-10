#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    namespace NSyncer {
        class TSyncerJobTask;
    }
    class TSyncerContext;

    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerJobDone
    ////////////////////////////////////////////////////////////////////////////
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
