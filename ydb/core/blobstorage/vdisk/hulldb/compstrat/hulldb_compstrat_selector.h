#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

#include <util/stream/file.h>

namespace NKikimr {
    namespace NHullComp {


        // FIXME:
        // When writing the compacted SST we need to take into account with how
        // many SSTs on the next level we intersect (don't make intersection with more
        // than 10 SSTs). If intersection happens with more table, finish writing
        // current SST and start a new one


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategy
        // It calculates what to compact next
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategy {
        public:
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;

            TStrategy(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TSelectorParams &params,
                    TLevelIndexSnapshot &&levelSnap,
                    TBarriersSnapshot &&barriersSnap,
                    TTask *task)
                : HullCtx(hullCtx)
                , LevelSnap(std::move(levelSnap))
                , BarriersSnap(std::move(barriersSnap))
                , Task(task)
                , Params(params)
            {
                Y_VERIFY_DEBUG(Task);
                Task->Clear();
                Task->FullCompactionInfo.first = Params.FullCompactionAttrs;
            }

            // Select an action to perform
            EAction Select();

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            TLevelIndexSnapshot LevelSnap;
            TBarriersSnapshot BarriersSnap;
            TTask *Task;
            TSelectorParams Params;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSelected
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        struct TSelected :
            public TEventLocal<TSelected<TKey, TMemRec>, TEvBlobStorage::EvHullCompSelected>
        {
            typedef ::NKikimr::NHullComp::TTask<TKey, TMemRec> TCompactionTask;

            const NHullComp::EAction Action;
            std::unique_ptr<TCompactionTask> CompactionTask;

            TSelected(NHullComp::EAction action, std::unique_ptr<TCompactionTask> compactionTask)
                : Action(action)
                , CompactionTask(std::move(compactionTask))
            {}
        };


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TSelectorActor
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TSelectorActor : public TActorBootstrapped<TSelectorActor<TKey, TMemRec>> {
            typedef TSelectorActor<TKey, TMemRec> TThis;
            typedef ::NKikimr::TLevelIndex<TKey, TMemRec> TLevelIndex;
            typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
            typedef ::NKikimr::NHullComp::TTask<TKey, TMemRec> TCompactionTask;
            typedef ::NKikimr::NHullComp::TStrategy<TKey, TMemRec> TStrategy;
            typedef ::NKikimr::NHullComp::TSelected<TKey, TMemRec> TSelected;


            friend class TActorBootstrapped<TThis>;

            TIntrusivePtr<THullCtx> HullCtx;
            TSelectorParams Params;
            TLevelIndexSnapshot LevelSnap;
            TBarriersSnapshot BarriersSnap;
            const TActorId RecipientID;
            std::unique_ptr<TCompactionTask> CompactionTask;

            void Bootstrap(const TActorContext &ctx) {
                TInstant startTime(TAppData::TimeProvider->Now());
                TStrategy strategy(HullCtx, Params, std::move(LevelSnap), std::move(BarriersSnap),
                        CompactionTask.get());

                NHullComp::EAction action = strategy.Select();
                ctx.Send(RecipientID, new TSelected(action, std::move(CompactionTask)));

                TInstant finishTime(TAppData::TimeProvider->Now());
                LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                         VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: Selector actor: action# %s timeSpent# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data()));
                TThis::Die(ctx);
            }

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_HULLCOMP_SELECTOR;
            }

            TSelectorActor(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TSelectorParams &params,
                    TLevelIndexSnapshot &&levelSnap,
                    TBarriersSnapshot &&barriersSnap,
                    const TActorId &recipientID,
                    std::unique_ptr<TCompactionTask> compactionTask)
                : TActorBootstrapped<TThis>()
                , HullCtx(hullCtx)
                , Params(params)
                , LevelSnap(std::move(levelSnap))
                , BarriersSnap(std::move(barriersSnap))
                , RecipientID(recipientID)
                , CompactionTask(std::move(compactionTask))
            {}
        };

    } // NHullComp

} // NKikimr
