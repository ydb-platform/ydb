#include "defs.h"
#include "blobstorage_syncfull.h"
#include <ydb/core/util/frequently_called_hptimer.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_snapshot.h>
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
    // THullSyncFullBase
    ////////////////////////////////////////////////////////////////////////////
    class THullSyncFullBase {
    protected:
        TIntrusivePtr<TVDiskConfig> Config;
        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        THullDsSnap FullSnap;

        const TSyncState SyncState;
        const TVDiskID SelfVDiskId;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        std::shared_ptr<NMonGroup::TFullSyncGroup> FullSyncGroup;
        TEvBlobStorage::TEvVSyncFull::TPtr CurrentEvent;
        std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> Result;

        // filters for record processing
        TFakeFilter FakeFilter;
        TLogoBlobFilterForHull LogoBlobFilter;

        constexpr static TDuration MaxProcessingTime = TDuration::MilliSeconds(5);  // half of a quota for mailbox

    protected:
        // keys are subject to change during the processing
        TKeyLogoBlob KeyLogoBlob;
        TKeyBlock KeyBlock;
        TKeyBarrier KeyBarrier;
        NKikimrBlobStorage::ESyncFullStage Stage;

    protected:
        void Serialize(TString *buf,
                       const TKeyLogoBlob &key,
                       const TMemRecLogoBlob &memRec) {
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetLogoBlob;
            ui32 size = s(HullCtx->VCtx->Top->GType, tmpBuf, 0, key.LogoBlobID(), memRec.GetIngress());
            buf->append(tmpBuf, size);
        }

        void Serialize(TString *buf,
                       const TKeyBlock &key,
                       const TMemRecBlock &memRec) {
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetBlock;
            ui32 size = s(tmpBuf, 0, key.TabletId, memRec.BlockedGeneration, 0);
            buf->append(tmpBuf, size);
        }

        void Serialize(TString *buf,
                       const TKeyBarrier &key,
                       const TMemRecBarrier &memRec) {
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetBarrier;
            ui32 size = s(tmpBuf, 0, key.TabletId, key.Channel, key.Gen,
                          key.GenCounter, memRec.CollectGen,
                          memRec.CollectStep, key.Hard, memRec.Ingress);
            buf->append(tmpBuf, size);
        }

        static const ui32 EmptyFlag = 0x1;
        static const ui32 MsgFullFlag = 0x2;
        static const ui32 LongProcessing = 0x4;

        template <class TKey, class TMemRec, class TFilter>
        ui32 Process(
                ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>& snapshot,
                TKey& key,
                const TFilter& filter,
                TString* data) {

            // reserve some space for data
            if (data->capacity() < Config->MaxResponseSize) {
                data->reserve(Config->MaxResponseSize);
            }

            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TIndexForwardIterator = typename TLevelIndexSnapshot::TIndexForwardIterator;
            TIndexForwardIterator it(HullCtx, &snapshot);
            it.Seek(key);

            TFrequentlyCalledHPTimer timer(MaxProcessingTime); 

            // copy data until we have some space
            ui32 result = 0;
            while (it.Valid()) {
                if (data->size() + NSyncLog::MaxRecFullSize > data->capacity()) {
                    result |= MsgFullFlag;
                    break;
                }

                if (timer.Check()) {
                    result |= LongProcessing;
                    break;
                }

                key = it.GetCurKey();
                if (filter.Check(key, it.GetMemRec(), HullCtx->AllowKeepFlags, true /*allowGarbageCollection*/))
                    Serialize(data, key, it.GetMemRec());
                it.Next();
            }
            // key points to the last seen key

            if (!it.Valid())
                result |= EmptyFlag;

            return result;
        }

        virtual ui32 ProcessPhantomFlags(TString* data) = 0;

        std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> RunStages(const TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
            LogoBlobFilter.BuildBarriersEssence(FullSnap.BarriersSnap);

            std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result =
                    std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(
                            NKikimrProto::OK, SelfVDiskId, SyncState,
                            ev->Get()->Record.GetCookie(), TActivationContext::Now(),
                            IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, ev->GetChannel());

            TString* data = result->Record.MutableData();
            ui32 pres = 0;
            switch (Stage) {
                case NKikimrBlobStorage::LogoBlobs:
                    Stage = NKikimrBlobStorage::LogoBlobs;
                    pres = Process(FullSnap.LogoBlobsSnap, KeyLogoBlob, LogoBlobFilter, data);
                    if (pres & (MsgFullFlag | LongProcessing))
                        break;
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    [[fallthrough]];
                case NKikimrBlobStorage::Blocks:
                    Stage = NKikimrBlobStorage::Blocks;
                    pres = Process(FullSnap.BlocksSnap, KeyBlock, FakeFilter, data);
                    if (pres & (MsgFullFlag | LongProcessing))
                        break;
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    [[fallthrough]];
                case NKikimrBlobStorage::Barriers:
                    Stage = NKikimrBlobStorage::Barriers;
                    pres = Process(FullSnap.BarriersSnap, KeyBarrier, FakeFilter, data);
                    if (pres & (MsgFullFlag | LongProcessing))
                        break;
                    [[fallthrough]];
                case NKikimrBlobStorage::PhantomFlags:
                    Cerr << "PROCESS PHANTOM FLAGS" << Endl;
                    pres = ProcessPhantomFlags(data);
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    break;
                default: Y_ABORT("Unexpected case: stage=%d", Stage);
            }

            bool finished = (bool)(pres & EmptyFlag) && Stage == NKikimrBlobStorage::Barriers;

            // Status, SyncState, Data and VDiskID are already set up; set up other
            result->Record.SetFinished(finished);
            result->Record.SetStage(Stage);
            LogoBlobIDFromLogoBlobID(KeyLogoBlob.LogoBlobID(), result->Record.MutableLogoBlobFrom());
            result->Record.SetBlockTabletFrom(KeyBlock.TabletId);
            KeyBarrier.Serialize(*result->Record.MutableBarrierFrom());
            return result;
        }

    public:
        THullSyncFullBase(
                const TIntrusivePtr<TVDiskConfig> &config,
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                THullDsSnap &&fullSnap,
                const TSyncState& syncState,
                const TVDiskID& selfVDiskId,
                const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
                const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                const TEvBlobStorage::TEvVSyncFull::TPtr& ev,
                TKeyLogoBlob keyLogoBlob,
                TKeyBlock keyBlock,
                TKeyBarrier keyBarrier,
                NKikimrBlobStorage::ESyncFullStage stage)
            : Config(config)
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , FullSnap(std::move(fullSnap))
            , SyncState(syncState)
            , SelfVDiskId(selfVDiskId)
            , IFaceMonGroup(ifaceMonGroup)
            , FullSyncGroup(fullSyncGroup)
            , InitialEvent(ev)
            , FakeFilter()
            , LogoBlobFilter(HullCtx, VDiskIDFromVDiskID(InitialEvent->Get()->Record.GetSourceVDiskID()))
            , KeyLogoBlob(keyLogoBlob)
            , KeyBlock(keyBlock)
            , KeyBarrier(keyBarrier)
            , Stage(stage)
        {}

        virtual ~THullSyncFullBase() = default;
    };


    ////////////////////////////////////////////////////////////////////////////
    // THullSyncFullActorLegacyProtocol
    ////////////////////////////////////////////////////////////////////////////
    class THullSyncFullActorLegacyProtocol : public THullSyncFullBase,
            public TActorBootstrapped<THullSyncFullActorLegacyProtocol> {

    public:
        void Bootstrap() {
            std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result = RunStages(InitialEvent);
            // send reply
            SendVDiskResponse(TActivationContext::AsActorContext(), InitialEvent->Sender,
                    result.release(), 0, HullCtx->VCtx, {});
            // notify parent about death
            Send(ParentId, new TEvents::TEvGone);
            PassAway();
        }

        // We don't need Poison handler since actor dies right after Bootstrap
        // STRICT_STFUNC(StateFunc,
        //     HFunc(TEvents::TEvPoisonPill, HandlePoison)
        // )

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_SYNC_FULL;
        }

        virtual ui32 ProcessPhantomFlags(TString*) override {
            // legacy protocol doesn't work with PhantomFlagStorage
            return EmptyFlag;
        }

        THullSyncFullActorLegacyProtocol(
                const TIntrusivePtr<TVDiskConfig> &config,
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                THullDsSnap &&fullSnap,
                const TSyncState& syncState,
                const TVDiskID& selfVDiskId,
                const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
                const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                const TEvBlobStorage::TEvVSyncFull::TPtr& ev,
                const TKeyLogoBlob &keyLogoBlob,
                const TKeyBlock &keyBlock,
                const TKeyBarrier &keyBarrier,
                NKikimrBlobStorage::ESyncFullStage stage)
            : THullSyncFullBase(config, hullCtx, parentId, std::forward<THullDsSnap>(fullSnap),
                    syncState, selfVDiskId, ifaceMonGroup, fullSyncGroup, ev,
                    keyLogoBlob, keyBlock, keyBarrier, stage)
            , TActorBootstrapped<THullSyncFullActorLegacyProtocol>()
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullSyncFullActorUnorderedDataProtocol
    ////////////////////////////////////////////////////////////////////////////
    class THullSyncFullActorUnorderedDataProtocol : public THullSyncFullBase,
            public TActorBootstrapped<THullSyncFullActorUnorderedDataProtocol> {
    public:
        void Bootstrap() {
            ++FullSyncGroup->UnorderedDataProtocolActorsCreated();
            Become(&TThis::StateInit);
            Send(SyncLogActorId, new NSyncLog::TEvPhantomFlagStorageGetSnapshot);
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_SYNC_FULL_UNORDERED_DATA_PROTOCOL;
        }
    
        THullSyncFullActorUnorderedDataProtocol(
                const TIntrusivePtr<TVDiskConfig>& config,
                const TIntrusivePtr<THullCtx>& hullCtx,
                const TActorId& parentId,
                const TActorId& syncLogActorId,
                THullDsSnap&& fullSnap,
                const TSyncState& syncState,
                const TVDiskID& selfVDiskId,
                const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
                const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                const TEvBlobStorage::TEvVSyncFull::TPtr& ev)
            : THullSyncFullBase(config, hullCtx, parentId, std::forward<THullDsSnap>(fullSnap),
                    syncState, selfVDiskId, ifaceMonGroup, fullSyncGroup, ev, TKeyLogoBlob::First(), 
                    TKeyBlock::First(), TKeyBarrier::First(), NKikimrBlobStorage::LogoBlobs)
            , TActorBootstrapped<THullSyncFullActorUnorderedDataProtocol>()
            , SyncLogActorId(syncLogActorId)
        {}
    
    private:
        void Serialize(TString* buf, const NSyncLog::TLogoBlobRec& rec) {
            char tmpBuf[NSyncLog::MaxRecFullSize];
            auto s = NSyncLog::TSerializeRoutines::SetLogoBlob;
            ui32 size = s(HullCtx->VCtx->Top->GType, tmpBuf, 0, rec.LogoBlobID(), rec.Ingress);
            buf->append(tmpBuf, size);
        }

        virtual ui32 ProcessPhantomFlags(TString* data) override {
            TFrequentlyCalledHPTimer timer(MaxProcessingTime);
            Stage = NKikimrBlobStorage::PhantomFlags;

            // reserve some space for data
            if (data->capacity() < Config->MaxResponseSize) {
                data->reserve(Config->MaxResponseSize);
            }

            // copy data until we have some space
            ui32 result = 0;
            for (; PhantomFlagIterator != PhantomFlagStorageSnapshot->Flags.end(); ++PhantomFlagIterator) {
                if (data->size() + NSyncLog::MaxRecFullSize > data->capacity()) {
                    result |= MsgFullFlag;
                    break;
                }

                if (timer.Check()) {
                    result |= LongProcessing;
                    break;
                }

                if (true) { // TODO: borders
                    Serialize(data, *PhantomFlagIterator);
                }
            }

            // key points to the last seen key
            if (PhantomFlagIterator == PhantomFlagStorageSnapshot->Flags.end()) {
                result |= EmptyFlag;
            }

            return result;
        }

        void Handle(const TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
            std::unique_ptr<TEvBlobStorage::TEvVSyncFullResult> result = RunStages(ev);
            bool finished = result->Record.GetFinished();
            // send reply
            SendVDiskResponse(TActivationContext::AsActorContext(), ev->Sender, result.release(),
                    0, HullCtx->VCtx, {});
            // notify parent about death
            if (finished) {
                Send(ParentId, new TEvents::TEvGone);
                ++FullSyncGroup->UnorderedDataProtocolActorsTerminated();
                PassAway();
            }
        }

        void Handle(NSyncLog::TEvPhantomFlagStorageGetSnapshotResult::TPtr& ev) {
            PhantomFlagStorageSnapshot = std::move(ev->Get()->Snapshot);
            PhantomFlagIterator = PhantomFlagStorageSnapshot->Flags.begin();
            Become(&TThis::StateFunc);
            Handle(InitialEvent);
        }

        STRICT_STFUNC(StateInit,
            hFunc(NSyncLog::TEvPhantomFlagStorageGetSnapshotResult, Handle)
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
        )

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvVSyncFull, Handle)
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
            cFunc(TEvents::TEvWakeup::EventType, Run)
        )

    private:
        const TActorId SyncLogActorId;
        std::optional<NSyncLog::TPhantomFlagStorageSnapshot> PhantomFlagStorageSnapshot;
        NSyncLog::TPhantomFlags::iterator PhantomFlagIterator;
    };

    IActor *CreateHullSyncFullActorLegacyProtocol(
            const TIntrusivePtr<TVDiskConfig> &config,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorId &parentId,
            THullDsSnap &&fullSnap,
            const TSyncState& syncState,
            const TVDiskID& selfVDiskId,
            const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
            const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
            const TEvBlobStorage::TEvVSyncFull::TPtr& ev,
            const TKeyLogoBlob &keyLogoBlob,
            const TKeyBlock &keyBlock,
            const TKeyBarrier &keyBarrier,
            NKikimrBlobStorage::ESyncFullStage stage) {
        return new THullSyncFullActorLegacyProtocol(config, hullCtx, parentId,
                std::move(fullSnap), syncState, selfVDiskId, ifaceMonGroup,
                fullSyncGroup, ev, keyLogoBlob, keyBlock, keyBarrier, stage);
    }

    IActor* CreateHullSyncFullActorUnorderedDataProtocol(
            const TIntrusivePtr<TVDiskConfig> &config,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorId &parentId,
            const TActorId& syncLogActorId,
            THullDsSnap &&fullSnap,
            const TSyncState& syncState,
            const TVDiskID& selfVDiskId,
            const std::shared_ptr<NMonGroup::TVDiskIFaceGroup>& ifaceMonGroup,
            const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
            const TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
        return new THullSyncFullActorUnorderedDataProtocol(config, hullCtx, parentId,
                syncLogActorId, std::move(fullSnap), syncState, selfVDiskId, ifaceMonGroup,
                fullSyncGroup, ev);
    }

} // NKikimr
