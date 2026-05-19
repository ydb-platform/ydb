#include "hullop_compactfreshappendix.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_HULLCOMP

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TFreshAppendixCompaction
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TFreshAppendixCompaction : public TActorBootstrapped<TFreshAppendixCompaction<TKey, TMemRec>> {

        using TFreshData = ::NKikimr::TFreshData<TKey, TMemRec>;
        using TCompactionJob = typename TFreshData::TCompactionJob;
        using TFreshAppendixCompactionDone = ::NKikimr::TFreshAppendixCompactionDone<TKey, TMemRec>;
        using TThis = TFreshAppendixCompaction<TKey, TMemRec>;

        friend class TActorBootstrapped<TThis>;

        const TIntrusivePtr<TVDiskContext> VCtx;
        const TActorId Recipient;
        TCompactionJob Job;

        ///////////////////////// BOOTSTRAP ////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            auto startTime = TAppData::TimeProvider->Now();
            Job.Work();
            auto endTime = TAppData::TimeProvider->Now();

            YDB_LOG_CTX_INFO(ctx, ": FreshAppendix Compaction Job finished:",
                {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                {"data", PDiskSignatureForHullDbKey<TKey>().ToString().data()},
                {"duration", (endTime - startTime)});

            ctx.Send(Recipient, new TFreshAppendixCompactionDone(std::move(Job)));
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::VDISK_FRESH_APPENDIX_COMPACTION;
        }

        TFreshAppendixCompaction(const TIntrusivePtr<TVDiskContext> &vctx, TActorId recipient, TCompactionJob &&job)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , Recipient(recipient)
            , Job(std::move(job))
        {}
    };

   template <>
   void RunFreshAppendixCompaction<TKeyLogoBlob, TMemRecLogoBlob>(
           const TActorContext &ctx,
           const TIntrusivePtr<TVDiskContext> &vctx,
           TActorId recipient,
           typename ::NKikimr::TFreshData<TKeyLogoBlob, TMemRecLogoBlob>::TCompactionJob &&job)
   {
       using TCompaction = TFreshAppendixCompaction<TKeyLogoBlob, TMemRecLogoBlob>;
       RunInBatchPool(ctx, new TCompaction(vctx, recipient, std::move(job)));
   }

   template <>
   void RunFreshAppendixCompaction<TKeyBlock, TMemRecBlock>(
           const TActorContext &ctx,
           const TIntrusivePtr<TVDiskContext> &vctx,
           TActorId recipient,
           typename ::NKikimr::TFreshData<TKeyBlock, TMemRecBlock>::TCompactionJob &&job)
   {
        using TCompaction = TFreshAppendixCompaction<TKeyBlock, TMemRecBlock>;
        RunInBatchPool(ctx, new TCompaction(vctx, recipient, std::move(job)));
   }

   template <>
   void RunFreshAppendixCompaction<TKeyBarrier, TMemRecBarrier>(
           const TActorContext &ctx,
           const TIntrusivePtr<TVDiskContext> &vctx,
           TActorId recipient,
           typename ::NKikimr::TFreshData<TKeyBarrier, TMemRecBarrier>::TCompactionJob &&job)
   {
        using TCompaction = TFreshAppendixCompaction<TKeyBarrier, TMemRecBarrier>;
        RunInBatchPool(ctx, new TCompaction(vctx, recipient, std::move(job)));
   }

} // NKikimr
