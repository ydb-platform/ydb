#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullChange
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TFreshAppendixCompactionDone
        : public TEventLocal<TFreshAppendixCompactionDone<TKey, TMemRec>,
                TEvBlobStorage::EvFreshAppendixCompactionDone>
    {
        using TFreshData = ::NKikimr::TFreshData<TKey, TMemRec>;
        using TCompactionJob = typename TFreshData::TCompactionJob;

        // Compaction Job with Work() method already called on it
        TCompactionJob Job;

        TFreshAppendixCompactionDone(TCompactionJob &&job)
            : Job(std::move(job))
        {}
    };

   template <class TKey, class TMemRec>
   void RunFreshAppendixCompaction(const TActorContext &ctx, const TIntrusivePtr<TVDiskContext> &vctx,
       TActorId recipient, typename ::NKikimr::TFreshData<TKey, TMemRec>::TCompactionJob &&job);

} // namespace NKikimr
