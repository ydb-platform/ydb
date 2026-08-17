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
        enum class ERunStagesResult : ui8 {
            PortionComplete,
            Yield,
            WaitForEvent,
        };

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
        static const ui32 YieldedFlag = 0x4;
        static const ui32 WaitFlag = 0x8;

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
                key = it.GetCurKey();

                if (data->size() + NSyncLog::MaxRecFullSize > data->capacity()) {
                    return MsgFullFlag;
                }

                if (timer.Check()) {
                    return YieldedFlag;
                }

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
        virtual NKikimrBlobStorage::EFullSyncProtocol GetProtocol() = 0;

        ERunStagesResult RunStages() {
            if (!Result) {
                Result = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(NKikimrProto::OK, SelfVDiskId,
                        SyncState, CurrentEvent->Get()->Record.GetCookie(), TActivationContext::Now(),
                        IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, CurrentEvent->GetChannel(),
                        GetProtocol());
                LogoBlobFilter.BuildBarriersEssence(FullSnap.BarriersSnap);
            }

            TString* data = Result->Record.MutableData();
            ui32 pres = 0;
            switch (Stage) {
                case NKikimrBlobStorage::LogoBlobs:
                    Stage = NKikimrBlobStorage::LogoBlobs;
                    pres = Process(FullSnap.LogoBlobsSnap, KeyLogoBlob, LogoBlobFilter, data);
                    if (pres & MsgFullFlag) {
                        break;
                    } else if (pres & YieldedFlag) {
                        return ERunStagesResult::Yield;
                    }
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    [[fallthrough]];
                case NKikimrBlobStorage::Blocks:
                    Stage = NKikimrBlobStorage::Blocks;
                    pres = Process(FullSnap.BlocksSnap, KeyBlock, FakeFilter, data);
                    if (pres & MsgFullFlag) {
                        break;
                    } else if (pres & YieldedFlag) {
                        return ERunStagesResult::Yield;
                    }
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    [[fallthrough]];
                case NKikimrBlobStorage::Barriers:
                    Stage = NKikimrBlobStorage::Barriers;
                    pres = Process(FullSnap.BarriersSnap, KeyBarrier, FakeFilter, data);
                    if (pres & MsgFullFlag) {
                        break;
                    } else if (pres & YieldedFlag) {
                        return ERunStagesResult::Yield;
                    }
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    [[fallthrough]];
                case NKikimrBlobStorage::PhantomFlags:
                    pres = ProcessPhantomFlags(data);
                    if (pres & MsgFullFlag) {
                        break;
                    } else if (pres & YieldedFlag) {
                        return ERunStagesResult::Yield;
                    } else if (pres & WaitFlag) {
                        return ERunStagesResult::WaitForEvent;
                    }
                    Y_VERIFY_S(pres & EmptyFlag, HullCtx->VCtx->VDiskLogPrefix);
                    break;
                default:
                    Y_ABORT("Unexpected case: stage=%d", Stage);
            }

            bool finished = false;

            if (pres & EmptyFlag) {
                switch (GetProtocol()) {
                case NKikimrBlobStorage::EFullSyncProtocol::Legacy:
                    finished = (Stage == NKikimrBlobStorage::Barriers);
                    break;
                case NKikimrBlobStorage::EFullSyncProtocol::UnorderedData:
                    finished = (Stage == NKikimrBlobStorage::PhantomFlags);
                    break;
                }
            }

            // Status, SyncState, Data and VDiskID are already set up; set up other
            Result->Record.SetFinished(finished);
            Result->Record.SetStage(Stage);
            LogoBlobIDFromLogoBlobID(KeyLogoBlob.LogoBlobID(), Result->Record.MutableLogoBlobFrom());
            Result->Record.SetBlockTabletFrom(KeyBlock.TabletId);
            KeyBarrier.Serialize(*Result->Record.MutableBarrierFrom());
            return ERunStagesResult::PortionComplete;
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
            , CurrentEvent(ev)
            , FakeFilter()
            , LogoBlobFilter(HullCtx, VDiskIDFromVDiskID(CurrentEvent->Get()->Record.GetSourceVDiskID()))
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
            Become(&TThis::StateFunc);
            Run();
        }

        void Handle(const TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
            CurrentEvent = ev;
            Run();
        }

        void Run() {
            ERunStagesResult portionCompleted = RunStages();
            switch (portionCompleted) {
            case ERunStagesResult::PortionComplete:
                SendVDiskResponse(TActivationContext::AsActorContext(), CurrentEvent->Sender,
                        Result.release(), 0, HullCtx->VCtx, {});
                // notify parent about death
                Send(ParentId, new TEvents::TEvGone);
                PassAway();
                return;
            case ERunStagesResult::WaitForEvent:
                break;
            case ERunStagesResult::Yield:
                Schedule(TDuration::Zero(), new TEvents::TEvWakeup);
                break;
            default:
                Y_ABORT();
            }
        }

        ui32 ProcessPhantomFlags(TString*) override {
            return EmptyFlag;
        }

        NKikimrBlobStorage::EFullSyncProtocol GetProtocol() override {
            return NKikimrBlobStorage::EFullSyncProtocol::Legacy;
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
            cFunc(TEvents::TEvWakeup::EventType, Run)
        )

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_SYNC_FULL;
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
            Become(&TThis::StateFunc);
            Run();
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

        ui32 ProcessPhantomFlags(TString* data) override {
            TFrequentlyCalledHPTimer timer(MaxProcessingTime);
            Stage = NKikimrBlobStorage::PhantomFlags;

            // reserve some space for data
            if (data->capacity() < Config->MaxResponseSize) {
                data->reserve(Config->MaxResponseSize);
            }

            // Drain the current batch.
            while (!PhantomFlagsQueue.empty()) {
                if (data->size() + NSyncLog::MaxRecFullSize > data->capacity()) {
                    return MsgFullFlag;
                }
                if (timer.Check()) {
                    return YieldedFlag;
                }
                Serialize(data, PhantomFlagsQueue.front());
                PhantomFlagsQueue.pop_front();
            }

            // Queue drained.  If more batches are still coming, request the
            // next one and yield until it arrives.
            if (!StreamFinished) {
                RequestNextSnapshotBatch();
                return WaitFlag;
            }

            return EmptyFlag;
        }

        NKikimrBlobStorage::EFullSyncProtocol GetProtocol() override {
            return NKikimrBlobStorage::EFullSyncProtocol::UnorderedData;
        }

        void Handle(TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
            CurrentEvent = ev;
            Run();
        }

        void Run() {
            switch (RunStages()) {
            case ERunStagesResult::PortionComplete: {
                bool finished = Result->Record.GetFinished();
                // send reply
                SendVDiskResponse(TActivationContext::AsActorContext(), CurrentEvent->Sender, Result.release(),
                        0, HullCtx->VCtx, {});
                // notify parent about death
                if (finished) {
                    Send(ParentId, new TEvents::TEvGone);
                    ++FullSyncGroup->UnorderedDataProtocolActorsTerminated();
                    PassAway();
                    return;
                }
                break;
            }
            case ERunStagesResult::Yield:
                Schedule(TDuration::Zero(), new TEvents::TEvWakeup);
                break;
            case ERunStagesResult::WaitForEvent:
                break;
            default:
                Y_ABORT();
            }
        }

        void Handle(NSyncLog::TEvPhantomFlagStorageGetSnapshotResult::TPtr& ev) {
            auto* msg = ev->Get();
            std::move(msg->Flags.begin(), msg->Flags.end(), std::back_inserter(PhantomFlagsQueue));
            ProcessedChunks = std::move(msg->ProcessedChunks);
            StreamFinished = msg->Eof;
            Run();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NSyncLog::TEvPhantomFlagStorageGetSnapshotResult, Handle)
            hFunc(TEvBlobStorage::TEvVSyncFull, Handle)
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway)
            cFunc(TEvents::TEvWakeup::EventType, Run)
        )

    private:
        void RequestNextSnapshotBatch() {
            auto req = std::make_unique<NSyncLog::TEvPhantomFlagStorageGetSnapshot>();
            req->ProcessedChunks = ProcessedChunks;
            Send(SyncLogActorId, req.release());
        }

    private:
        const TActorId SyncLogActorId;
        std::deque<NSyncLog::TLogoBlobRec> PhantomFlagsQueue;
        std::unordered_set<ui32> ProcessedChunks;
        bool StreamFinished = false;
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
