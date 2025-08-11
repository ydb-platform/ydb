#pragma once

#include "blobstorage_hulllog.h"
#include "hullop_entryserialize.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>

namespace NKikimr {


    ////////////////////////////////////////////////////////////////////////////////
    // TBaseHullDbCommitter
    ////////////////////////////////////////////////////////////////////////////////
    class THullDbCommitterCtx {
    public:
        TPDiskCtxPtr PDiskCtx;
        THullCtxPtr HullCtx;
        TIntrusivePtr<TLsnMngr> LsnMngr;
        const TActorId LoggerId;
        const TActorId HugeKeeperId;
        const TActorId SkeletonId;

        THullDbCommitterCtx(
                TPDiskCtxPtr pdiskCtx,
                THullCtxPtr hullCtx,
                TIntrusivePtr<TLsnMngr> lsnMngr,
                const TActorId loggerId,
                const TActorId hugeKeeperId,
                const TActorId skeletonId)
            : PDiskCtx(std::move(pdiskCtx))
            , HullCtx(std::move(hullCtx))
            , LsnMngr(std::move(lsnMngr))
            , LoggerId(loggerId)
            , HugeKeeperId(hugeKeeperId)
            , SkeletonId(skeletonId)
        {
            Y_ABORT_UNLESS(PDiskCtx && HullCtx && LsnMngr && LoggerId && HugeKeeperId && SkeletonId);
        }
    };

    using THullDbCommitterCtxPtr = std::shared_ptr<THullDbCommitterCtx>;

    ////////////////////////////////////////////////////////////////////////////////
    // TBaseHullDbCommitter
    ////////////////////////////////////////////////////////////////////////////////
    template<typename TKey, typename TMemRec, THullCommitFinished::EType NotifyType,
        NKikimrServices::TActivity::EType DerivedActivityType>
    class TBaseHullDbCommitter
        : public TActorBootstrapped<TBaseHullDbCommitter<TKey, TMemRec, NotifyType, DerivedActivityType>>
    {
    protected:
        friend class TActorBootstrapped<TBaseHullDbCommitter<TKey, TMemRec, NotifyType, DerivedActivityType>>;

        using TLevelIndex = NKikimr::TLevelIndex<TKey, TMemRec>;
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
        using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
        using TThis = TBaseHullDbCommitter<TKey, TMemRec, NotifyType, DerivedActivityType>;

        struct THullCommitMeta {
            TVector<ui32>    CommitChunks;      // chunks to commit within this log entry
            TVector<ui32>    DeleteChunks;      // chunks to delete
            TDiskPartVec     RemovedHugeBlobs;  // freed huge blobs
            TDiskPartVec     AllocatedHugeBlobs;
            TLevelSegmentPtr ReplSst;           // pointer to replicated SST
            ui32             NumRecoveredBlobs; // number of blobs in this SST (valid only for replicated tables)
            bool             DeleteToDecommitted;

            // constructor for ordinary committer (advance, fresh, level)
            THullCommitMeta(TVector<ui32>&& chunksAdded,
                            TVector<ui32>&& chunksDeleted,
                            TDiskPartVec&&  removedHugeBlobs,
                            TDiskPartVec&&  allocatedHugeBlobs,
                            bool            prevSliceActive)
                : CommitChunks(std::move(chunksAdded))
                , DeleteChunks(std::move(chunksDeleted))
                , RemovedHugeBlobs(std::move(removedHugeBlobs))
                , AllocatedHugeBlobs(std::move(allocatedHugeBlobs))
                , NumRecoveredBlobs(0)
                , DeleteToDecommitted(prevSliceActive)
            {}

            // constructor for repl sst committer
            THullCommitMeta(TVector<ui32>&&  chunksAdded,
                            TVector<ui32>&&  chunksDeleted,
                            TLevelSegmentPtr replSst,
                            ui32             numRecoveredBlobs)
                : CommitChunks(std::move(chunksAdded))
                , DeleteChunks(std::move(chunksDeleted))
                , ReplSst(std::move(replSst))
                , NumRecoveredBlobs(numRecoveredBlobs)
                , DeleteToDecommitted(false)
            {}
        };

        std::shared_ptr<THullLogCtx> HullLogCtx;
        THullDbCommitterCtxPtr Ctx;
        TIntrusivePtr<TLevelIndex> LevelIndex;
        TActorId NotifyID;
        TActorId SecondNotifyID;
        THullCommitMeta Metadata;
        std::unique_ptr<NPDisk::TEvLog> CommitMsg;
        TLsnSeg LsnSeg;
        NPDisk::TCommitRecord CommitRecord;
        TStringStream DebugMessage;
        TString CallerInfo;
        const ui64 WId;

        void Bootstrap(const TActorContext& ctx) {
            TThis::Become(&TThis::StateFunc);
            LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                    VDISKP(HullLogCtx->VCtx->VDiskLogPrefix, "sending %s lsn# %" PRIu64 " %s",
                        THullCommitFinished::TypeToString(NotifyType), CommitMsg->Lsn, CommitMsg->ToString().data()));

            if (CommitRecord.CommitChunks || CommitRecord.DeleteChunks) {
                LOG_INFO(ctx, NKikimrServices::BS_SKELETON,
                        VDISKP(HullLogCtx->VCtx->VDiskLogPrefix, "commit %s signature# %s CommitChunks# %s"
                            " DeleteChunks# %s", THullCommitFinished::TypeToString(NotifyType),
                            PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                            FormatList(CommitRecord.CommitChunks).data(),
                            FormatList(CommitRecord.DeleteChunks).data()));
            }

            LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS,
                      VDISKP(HullLogCtx->VCtx->VDiskLogPrefix, "COMMIT: PDiskId# %s Lsn# %s type# %s msg# %s RemovedHugeBlobs# %s",
                            Ctx->PDiskCtx->PDiskIdString.data(), LsnSeg.ToString().data(),
                            THullCommitFinished::TypeToString(NotifyType), CommitMsg->CommitRecord.ToString().data(),
                            Metadata.RemovedHugeBlobs.ToString().data()));

            ctx.Send(Ctx->LoggerId, CommitMsg.release());
        }

        virtual STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvLogResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

        void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
            CHECK_PDISK_RESPONSE(Ctx->HullCtx->VCtx, ev, ctx);

            // notify delayed deleter when log record is actually written; we MUST ensure that updates are coming in
            // order of increasing LSN's; this is achieved automatically as all actors reside on the same mailbox
            LevelIndex->DelayedCompactionDeleterInfo->Update(LsnSeg.Last, std::move(Metadata.RemovedHugeBlobs),
                std::move(Metadata.AllocatedHugeBlobs), CommitRecord.DeleteToDecommitted ? CommitRecord.DeleteChunks :
                TVector<TChunkIdx>(), PDiskSignatureForHullDbKey<TKey>(), WId, ctx, Ctx->HugeKeeperId, Ctx->SkeletonId,
                Ctx->PDiskCtx, Ctx->HullCtx->VCtx);

            NPDisk::TEvLogResult* msg = ev->Get();

            // notify descendants about successful commit
            OnLogResult(msg);

            // update current entry point for desired level index
            const auto& results = msg->Results;
            Y_DEBUG_ABORT_UNLESS(results.size() == 1 && results.front().Lsn == LsnSeg.Last);

            LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                     VDISKP(HullLogCtx->VCtx->VDiskLogPrefix, "%s lsn# %s done wId# %" PRIu64,
                        THullCommitFinished::TypeToString(NotifyType), LsnSeg.ToString().data(), WId));

            LOG_INFO(ctx, NKikimrServices::BS_HULLRECS,
                    VDISKP(HullLogCtx->VCtx->VDiskLogPrefix, "%s", DebugMessage.Str().data()));

            // advance LSN
            LevelIndex->CurEntryPointLsn = LsnSeg.Last;

            if (CommitRecord.DeleteChunks) {
                ctx.Send(Ctx->SkeletonId, new TEvNotifyChunksDeleted(LsnSeg.Last, CommitRecord.DeleteChunks));
            }

            Finish(ctx);
        }

        void Finish(const TActorContext& ctx) {
            // if this was replicated SST, put it into hull -- now it is visible for users
            if (Metadata.ReplSst) {
                Ctx->LsnMngr->ConfirmLsnForHull(LsnSeg, false);
                LevelIndex->ApplyUncommittedReplSegment(std::move(Metadata.ReplSst), Ctx->HullCtx);
            }

            // notify sender & die
            ctx.Send(NotifyID, new THullCommitFinished(NotifyType));
            if (SecondNotifyID)
                ctx.Send(SecondNotifyID, new TEvAddBulkSstResult);
            TThis::Die(ctx);
        }

        // validate commit record contents; this function may change order of CommitChunks/DeleteChunks inside commit
        // record, but this doesn't matter for PDisk
        void VerifyCommitRecord(NPDisk::TCommitRecord& commitRecord) {
            // sort set of chunks to quickly perform further checks
            std::sort(commitRecord.CommitChunks.begin(), commitRecord.CommitChunks.end());
            std::sort(commitRecord.DeleteChunks.begin(), commitRecord.DeleteChunks.end());

            // verify that chunk ids do not repeat in both of arrays
            Y_VERIFY_S(std::adjacent_find(commitRecord.CommitChunks.begin(), commitRecord.CommitChunks.end()) ==
                    commitRecord.CommitChunks.end(),
                    HullLogCtx->VCtx->VDiskLogPrefix);
            Y_VERIFY_S(std::adjacent_find(commitRecord.DeleteChunks.begin(), commitRecord.DeleteChunks.end()) ==
                    commitRecord.DeleteChunks.end(),
                    HullLogCtx->VCtx->VDiskLogPrefix);

            // ensure that there are no intersections between chunks being committed and deleted
            TVector<TChunkIdx> isect;
            std::set_intersection(commitRecord.CommitChunks.begin(), commitRecord.CommitChunks.end(),
                    commitRecord.DeleteChunks.begin(), commitRecord.DeleteChunks.end(),
                    std::back_inserter(isect));
            Y_VERIFY_S(isect.empty(), HullLogCtx->VCtx->VDiskLogPrefix);
        }

        void VerifyRemovedHugeBlobs(TDiskPartVec& v) {
            auto comp = [](const TDiskPart& x, const TDiskPart& y) {
                return std::make_tuple(x.ChunkIdx, x.Offset, x.Size) < std::make_tuple(y.ChunkIdx, y.Offset, y.Size);
            };
            std::sort(v.Vec.begin(), v.Vec.end(), comp);

            auto pred = [](const TDiskPart& x, const TDiskPart& y) {
                return x.ChunkIdx == y.ChunkIdx && x.Offset == y.Offset;
            };
            auto it = std::adjacent_find(v.Vec.begin(), v.Vec.end(), pred);
            if (it != v.end()) {
                auto second = std::next(it);
                Y_ABORT_S(HullLogCtx->VCtx->VDiskLogPrefix
                    << "duplicate removed huge slots: x#" << it->ToString() << " y# " << second->ToString());
            }
        }

        TString GenerateEntryPointData() const {
            // prepare log record data
            NKikimrVDiskData::THullDbEntryPoint pb;
            LevelIndex->SerializeToProto(*pb.MutableLevelIndex());
            Metadata.RemovedHugeBlobs.SerializeToProto(*pb.MutableRemovedHugeBlobs());
            Metadata.AllocatedHugeBlobs.SerializeToProto(*pb.MutableAllocatedHugeBlobs());
            pb.SetHullCompLevel0MaxSstsAtOnce(Ctx->HullCtx->HullCompLevel0MaxSstsAtOnce);
            pb.SetHullCompSortedPartsNum(Ctx->HullCtx->HullCompSortedPartsNum);
            return THullDbSignatureRoutines::Serialize(pb);
        }

        void GenerateCommitMessage() {
            // prepare commit record
            CommitRecord.IsStartingPoint = true;
            CommitRecord.CommitChunks = std::move(Metadata.CommitChunks);
            CommitRecord.DeleteChunks = std::move(Metadata.DeleteChunks);
            CommitRecord.DeleteToDecommitted = Metadata.DeleteToDecommitted;

            // notify PDisk about dirty chunks (the ones from which huge slots are being freed right now)
            THashSet<TChunkIdx> chunkIds;
            for (const TDiskPart& p : Metadata.RemovedHugeBlobs) {
                chunkIds.insert(p.ChunkIdx);
            }
            CommitRecord.DirtyChunks = {chunkIds.begin(), chunkIds.end()};

            // validate its contents
            VerifyCommitRecord(CommitRecord);
            VerifyRemovedHugeBlobs(Metadata.RemovedHugeBlobs);

            // create commit message
            if (Metadata.ReplSst) {
                // for replicated SST -- generate LSN range; do it now, because in serialization we need actual data
                // generate range of LSN's covering newly generated blobs
                const ui64 lsnAdvance = Metadata.NumRecoveredBlobs;
                Y_VERIFY_S(lsnAdvance > 0, HullLogCtx->VCtx->VDiskLogPrefix);
                LsnSeg = Ctx->LsnMngr->AllocLsnForHull(lsnAdvance);
                // store first/last LSN into level segment
                Metadata.ReplSst->Info.FirstLsn = LsnSeg.First;
                Metadata.ReplSst->Info.LastLsn = LsnSeg.Last;
                // generate entry point data when LsgSeg is already allocated
                TString data = GenerateEntryPointData();
                // create sync log message covering this segment; it will be issued when log entry is written
                CommitMsg = CreateHullUpdate(HullLogCtx, PDiskSignatureForHullDbKey<TKey>(), CommitRecord,
                    data, LsnSeg, nullptr, nullptr);
            } else {
                LsnSeg = Ctx->LsnMngr->AllocLsnForLocalUse();
                DebugMessage << "Db# " << TKey::Name()
                    << " Log entry point: LevelIndex# " << LevelIndex->ToString()
                    << " lsn# " << LsnSeg.ToString();
                if (CallerInfo) {
                    DebugMessage << " caller# " << CallerInfo;
                }
                TRcBuf data = TRcBuf(GenerateEntryPointData());
                CommitMsg = std::make_unique<NPDisk::TEvLog>(Ctx->PDiskCtx->Dsk->Owner, Ctx->PDiskCtx->Dsk->OwnerRound,
                    PDiskSignatureForHullDbKey<TKey>(), CommitRecord, data, LsnSeg, nullptr);
            }
        }

        virtual void OnLogResult(NPDisk::TEvLogResult* /*msg*/) {}

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return DerivedActivityType;
        }

        TBaseHullDbCommitter(
                std::shared_ptr<THullLogCtx> hullLogCtx,
                THullDbCommitterCtxPtr ctx,
                TIntrusivePtr<TLevelIndex> levelIndex,
                const TActorId& notifyID,
                const TActorId& secondNotifyID,
                THullCommitMeta&& metadata,
                const TString &callerInfo,
                ui64 wId)
            : HullLogCtx(std::move(hullLogCtx))
            , Ctx(std::move(ctx))
            , LevelIndex(std::move(levelIndex))
            , NotifyID(notifyID)
            , SecondNotifyID(secondNotifyID)
            , Metadata(std::move(metadata))
            , CallerInfo(callerInfo)
            , WId(wId)
        {
            Y_VERIFY_S(!WId == Metadata.RemovedHugeBlobs.Empty(), HullLogCtx->VCtx->VDiskLogPrefix);
            // we create commit message in the constructor to avoid race condition
            GenerateCommitMessage();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAsyncAdvanceLsnCommitter
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TAsyncAdvanceLsnCommitter
        : public TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitAdvanceLsn, NKikimrServices::TActivity::BS_ASYNC_LSN_COMMITTER>
    {
        using TBase = TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitAdvanceLsn, NKikimrServices::TActivity::BS_ASYNC_LSN_COMMITTER>;

    public:
        TAsyncAdvanceLsnCommitter(
                    std::shared_ptr<THullLogCtx> hullLogCtx,
                    THullDbCommitterCtxPtr ctx,
                    TIntrusivePtr<typename TBase::TLevelIndex> levelIndex,
                    const TActorId &notifyID,
                    const TString &callerInfo)
            : TBase(std::move(hullLogCtx),
                    std::move(ctx),
                    std::move(levelIndex),
                    notifyID,
                    TActorId(),
                    {TVector<ui32>(), TVector<ui32>(), TDiskPartVec(), TDiskPartVec(), false},
                    callerInfo,
                    0)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAsyncFreshCommitter
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TAsyncFreshCommitter :
        public TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitFresh, NKikimrServices::TActivity::BS_ASYNC_FRESH_COMMITTER>
    {
        using TBase = TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitFresh, NKikimrServices::TActivity::BS_ASYNC_FRESH_COMMITTER>;

        void OnLogResult(NPDisk::TEvLogResult* /*msg*/) override {
            TBase::LevelIndex->FreshCompactionFinished();
        }

    public:
        TAsyncFreshCommitter(
                std::shared_ptr<THullLogCtx> hullLogCtx,
                THullDbCommitterCtxPtr ctx,
                TIntrusivePtr<typename TBase::TLevelIndex> levelIndex,
                const TActorId& notifyID,
                TVector<ui32>&& chunksAdded,
                TVector<ui32>&& chunksDeleted,
                TDiskPartVec&& removedHugeBlobs,
                TDiskPartVec&& allocatedHugeBlobs,
                const TString &callerInfo,
                ui64 wId)
            : TBase(std::move(hullLogCtx),
                    std::move(ctx),
                    std::move(levelIndex),
                    notifyID,
                    TActorId(),
                    {std::move(chunksAdded), std::move(chunksDeleted), std::move(removedHugeBlobs), std::move(allocatedHugeBlobs), false},
                    callerInfo,
                    wId)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAsyncLevelCommitter
    ////////////////////////////////////////////////////////////////////////////

    template <class TKey, class TMemRec>
    class TAsyncLevelCommitter
        : public TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitLevel, NKikimrServices::TActivity::BS_ASYNC_LEVEL_COMMITTER>
    {
        using TBase = TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitLevel, NKikimrServices::TActivity::BS_ASYNC_LEVEL_COMMITTER>;

    public:
        TAsyncLevelCommitter(
                std::shared_ptr<THullLogCtx> hullLogCtx,
                THullDbCommitterCtxPtr ctx,
                TIntrusivePtr<typename TBase::TLevelIndex> levelIndex,
                const TActorId& notifyID,
                TVector<ui32>&& chunksAdded,
                TVector<ui32>&& chunksDeleted,
                TDiskPartVec&& removedHugeBlobs,
                TDiskPartVec&& allocatedHugeBlobs,
                ui64 wId)
            : TBase(std::move(hullLogCtx),
                    std::move(ctx),
                    std::move(levelIndex),
                    notifyID,
                    TActorId(),
                    {std::move(chunksAdded), std::move(chunksDeleted), std::move(removedHugeBlobs), std::move(allocatedHugeBlobs), true},
                    TString(),
                    wId)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////////
    // TAsyncReplSstCommitter
    ////////////////////////////////////////////////////////////////////////////////

    template<typename TKey, typename TMemRec>
    class TAsyncReplSstCommitter
        : public TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitReplSst, NKikimrServices::TActivity::BS_ASYNC_REPLSST_COMMITTER>
    {
        using TBase = TBaseHullDbCommitter<TKey, TMemRec, THullCommitFinished::CommitReplSst,  NKikimrServices::TActivity::BS_ASYNC_REPLSST_COMMITTER>;
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;

    public:
        TAsyncReplSstCommitter(
                std::shared_ptr<THullLogCtx> hullLogCtx,
                THullDbCommitterCtxPtr ctx,
                TIntrusivePtr<typename TBase::TLevelIndex> levelIndex,
                const TActorId& notifyID,
                TVector<ui32>&& chunksAdded,
                TVector<ui32>&& chunksDeleted,
                TIntrusivePtr<TLevelSegment> replSst,
                ui32 numRecoveredBlobs,
                const TActorId& secondNotifyID)
            : TBase(std::move(hullLogCtx),
                    std::move(ctx),
                    std::move(levelIndex),
                    notifyID,
                    secondNotifyID,
                    {std::move(chunksAdded), std::move(chunksDeleted), std::move(replSst), numRecoveredBlobs},
                    TString(),
                    0)
        {}
    };

} // NKikimr
