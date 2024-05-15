#include "defs.h"
#include "blobstorage_syncfull.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

using namespace NKikimrServices;

namespace NKikimr {

    struct TLogoBlobFilterForHull : public TLogoBlobFilter {
        TLogoBlobFilterForHull(const TIntrusivePtr<THullCtx> &hullCtx, const TVDiskID &vdisk)
            : TLogoBlobFilter(hullCtx->VCtx->Top, vdisk)
            , HullCtx(hullCtx)
        {}

        void BuildBarriersEssence(const TBarriersSnapshot &bsnap) {
            BarriersEssence = bsnap.CreateEssence(HullCtx);
        }

        bool Check(const TKeyLogoBlob &key,
                   const TMemRecLogoBlob &memRec,
                   bool allowKeepFlags,
                   bool allowGarbageCollection) const {
            return TLogoBlobFilter::Check(key.LogoBlobID()) && BarriersEssence->Keep(key, memRec, {},
                allowKeepFlags, allowGarbageCollection).KeepData;
        }

        TIntrusivePtr<THullCtx> HullCtx;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
    };


    ////////////////////////////////////////////////////////////////////////////
    // THullSyncFullActor
    ////////////////////////////////////////////////////////////////////////////
    class THullSyncFullActor : public TActorBootstrapped<THullSyncFullActor> {
        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        const TActorId Recipient;
        THullDsSnap FullSnap;
        // keys are subject to change during the processing
        TKeyLogoBlob KeyLogoBlob;
        TKeyBlock KeyBlock;
        TKeyBarrier KeyBarrier;
        NKikimrBlobStorage::ESyncFullStage Stage;
        std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> Result;
        TFakeFilter FakeFilter;
        TLogoBlobFilterForHull LogoBlobFilter;
        TVDiskID SourceVDisk;

        friend class TActorBootstrapped<THullSyncFullActor>;

        void Serialize(const TActorContext &ctx,
                       TString *buf,
                       const TKeyLogoBlob &key,
                       const TMemRecLogoBlob &memRec) {
            Y_UNUSED(ctx);
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetLogoBlob;
            ui32 size = s(HullCtx->VCtx->Top->GType, tmpBuf, 0, key.LogoBlobID(), memRec.GetIngress());
            buf->append(tmpBuf, size);
        }

        void Serialize(const TActorContext &ctx,
                       TString *buf,
                       const TKeyBlock &key,
                       const TMemRecBlock &memRec) {
            Y_UNUSED(ctx);
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetBlock;
            ui32 size = s(tmpBuf, 0, key.TabletId, memRec.BlockedGeneration, 0);
            buf->append(tmpBuf, size);
        }

        void Serialize(const TActorContext &ctx,
                       TString *buf,
                       const TKeyBarrier &key,
                       const TMemRecBarrier &memRec) {
            Y_UNUSED(ctx);
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetBarrier;
            ui32 size = s(tmpBuf, 0, key.TabletId, key.Channel, key.Gen,
                          key.GenCounter, memRec.CollectGen,
                          memRec.CollectStep, key.Hard, memRec.Ingress);
            buf->append(tmpBuf, size);
        }

        static const ui32 EmptyFlag = 0x1;
        static const ui32 MsgFullFlag = 0x2;

        void Bootstrap(const TActorContext &ctx) {
            LogoBlobFilter.BuildBarriersEssence(FullSnap.BarriersSnap);

            ui32 pres = 0;
            switch (Stage) {
                case NKikimrBlobStorage::LogoBlobs:
                    Stage = NKikimrBlobStorage::LogoBlobs;
                    pres = Process(ctx, FullSnap.LogoBlobsSnap, KeyLogoBlob, LogoBlobFilter);
                    if (pres & MsgFullFlag)
                        break;
                    Y_ABORT_UNLESS(pres & EmptyFlag);
                    [[fallthrough]];
                case NKikimrBlobStorage::Blocks:
                    Stage = NKikimrBlobStorage::Blocks;
                    pres = Process(ctx, FullSnap.BlocksSnap, KeyBlock, FakeFilter);
                    if (pres & MsgFullFlag)
                        break;
                    Y_ABORT_UNLESS(pres & EmptyFlag);
                    [[fallthrough]];
                case NKikimrBlobStorage::Barriers:
                    Stage = NKikimrBlobStorage::Barriers;
                    pres = Process(ctx, FullSnap.BarriersSnap, KeyBarrier, FakeFilter);
                    break;
                default: Y_ABORT("Unexpected case: stage=%d", Stage);
            }

            bool finished = (bool)(pres & EmptyFlag) && Stage == NKikimrBlobStorage::Barriers;

            // Status, SyncState, Data and VDiskID are already set up; set up other
            Result->Record.SetFinished(finished);
            Result->Record.SetStage(Stage);
            LogoBlobIDFromLogoBlobID(KeyLogoBlob.LogoBlobID(), Result->Record.MutableLogoBlobFrom());
            Result->Record.SetBlockTabletFrom(KeyBlock.TabletId);
            KeyBarrier.Serialize(*Result->Record.MutableBarrierFrom());
            // send reply
            SendVDiskResponse(ctx, Recipient, Result.release(), 0, HullCtx->VCtx);
            // notify parent about death
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            Die(ctx);
        }

        template <class TKey, class TMemRec, class TFilter>
        ui32 Process(
                const TActorContext &ctx,
                ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> &snapshot,
                TKey &key,
                const TFilter &filter) {
            // reserve some space for data
            TString *data = Result->Record.MutableData();
            if (data->capacity() < Config->MaxResponseSize) {
                data->reserve(Config->MaxResponseSize);
            }

            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TIndexForwardIterator = typename TLevelIndexSnapshot::TIndexForwardIterator;
            TIndexForwardIterator it(HullCtx, &snapshot);
            it.Seek(key);
            // copy data until we have some space
            while (it.Valid() && (data->size() + NSyncLog::MaxRecFullSize <= data->capacity())) {
                key = it.GetCurKey();
                if (filter.Check(key, it.GetMemRec(), HullCtx->AllowKeepFlags, true /*allowGarbageCollection*/))
                    Serialize(ctx, data, key, it.GetMemRec());
                it.Next();
            }
            // key points to the last seen key

            ui32 result = 0;
            if (!it.Valid())
                result |= EmptyFlag;
            if (data->size() + NSyncLog::MaxRecFullSize > data->capacity())
                result |= MsgFullFlag;
            return result;
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_SYNC_FULL;
        }

        THullSyncFullActor(
                const TIntrusivePtr<TVDiskConfig> &config,
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                const TVDiskID &sourceVDisk,
                const TActorId &recipient,
                THullDsSnap &&fullSnap,
                const TKeyLogoBlob &keyLogoBlob,
                const TKeyBlock &keyBlock,
                const TKeyBarrier &keyBarrier,
                NKikimrBlobStorage::ESyncFullStage stage,
                std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result)
            : TActorBootstrapped<THullSyncFullActor>()
            , Config(config)
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , Recipient(recipient)
            , FullSnap(std::move(fullSnap))
            , KeyLogoBlob(keyLogoBlob)
            , KeyBlock(keyBlock)
            , KeyBarrier(keyBarrier)
            , Stage(stage)
            , Result(std::move(result))
            , FakeFilter()
            , LogoBlobFilter(HullCtx, sourceVDisk)
            , SourceVDisk(sourceVDisk)
        {
            Y_UNUSED(SourceVDisk);
        }
    };


    IActor *CreateHullSyncFullActor(
            const TIntrusivePtr<TVDiskConfig> &config,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorId &parentId,
            const TVDiskID &sourceVDisk,
            const TActorId &recipient,
            THullDsSnap &&fullSnap,
            const TKeyLogoBlob &keyLogoBlob,
            const TKeyBlock &keyBlock,
            const TKeyBarrier &keyBarrier,
            NKikimrBlobStorage::ESyncFullStage stage,
            std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result) {
        return new THullSyncFullActor(config, hullCtx, parentId, sourceVDisk, recipient, std::move(fullSnap),
                                      keyLogoBlob, keyBlock, keyBarrier, stage, std::move(result));
    }

} // NKikimr
