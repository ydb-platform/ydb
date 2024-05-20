#include "blobstorage_hullhuge.h"
#include "blobstorage_hullhugerecovery.h"
#include "blobstorage_hullhugeheap.h"
#include "booltt.h"
#include "top.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/vdisk/common/align.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/library/actors/wilson/wilson_with_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/monlib/service/pages/templates.h>

using namespace NKikimrServices;
using namespace NKikimr::NHuge;

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

    ////////////////////////////////////////////////////////////////////////////
    // THugeBlobLogLsnFifo
    ////////////////////////////////////////////////////////////////////////////
    class THugeBlobLogLsnFifo {
    public:
        THugeBlobLogLsnFifo(ui64 seqWriteId = 0)
            : SeqWriteId(seqWriteId)
        {}

        ui64 Push(ui64 lsn) {
            Y_ABORT_UNLESS(Fifo.empty() || (--Fifo.end())->second <= lsn);
            if (NodeCache.empty()) {
                Fifo.emplace_hint(Fifo.end(), SeqWriteId, lsn);
                MaxFifoSize = Max(MaxFifoSize, Fifo.size());
            } else {
                auto& nh = NodeCache.back();
                nh.key() = SeqWriteId;
                nh.mapped() = lsn;
                Fifo.insert(Fifo.end(), std::move(nh));
                NodeCache.pop_back();
            }
            return SeqWriteId++;
        }

        void Pop(ui64 wId, ui64 lsn, bool logged) {
            const auto it = Fifo.find(wId);
            Y_ABORT_UNLESS(it != Fifo.end());
            Y_ABORT_UNLESS(!logged || it->second <= lsn);
            if (NodeCache.size() < 64) {
                NodeCache.push_back(Fifo.extract(it));
            } else {
                Fifo.erase(it);
            }
        }

        ui64 FirstLsnToKeep() const {
            return Fifo.empty() ? Max<ui64>() : Fifo.begin()->second;
        }

        TString FirstLsnToKeepDecomposed() const {
            const ui64 lsn = FirstLsnToKeep();
            return lsn != Max<ui64>() ? ::ToString(lsn) : "Max";
        }

        TString ToString() const {
            TStringStream s;
            s << "{SeqWriteId# " << SeqWriteId
                << " MaxFifoSize# " << MaxFifoSize;
            if (!Fifo.empty()) {
                auto it = Fifo.begin();
                s << " [" << it->first << "]# " << it->second;

                if (Fifo.size() > 2) {
                    ++it;
                    s << " [" << it->first << "]# " << it->second;
                }

                if (Fifo.size() > 3) {
                    s << " ...";
                }

                if (Fifo.size() > 1) {
                    it = --Fifo.end();
                    s << " [" << it->first << "]# " << it->second;
                }
            }
            s << "}";
            return s.Str();
        }

        size_t GetMaxFifoSize() const { return MaxFifoSize; }

    private:
        using TFifo = std::map<ui64, ui64>;

        TFifo Fifo;
        ui64 SeqWriteId = 0;
        std::vector<TFifo::node_type> NodeCache;
        size_t MaxFifoSize = 0;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullHugeChunkAllocated
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullHugeChunkAllocated : public TEventLocal<TEvHullHugeChunkAllocated, TEvBlobStorage::EvHullHugeChunkAllocated> {
    public:
        const ui32 ChunkId;
        const ui32 SlotSize;

        explicit TEvHullHugeChunkAllocated(ui32 chunkId, ui32 slotSize)
            : ChunkId(chunkId)
            , SlotSize(slotSize)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{ChunkId# " << ChunkId << " SlotSize# " << SlotSize << "}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullHugeChunkFreed
    ////////////////////////////////////////////////////////////////////////////
    struct TEvHullHugeChunkFreed : TEventLocal<TEvHullHugeChunkFreed, TEvBlobStorage::EvHullHugeChunkFreed> {};

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullHugeCommitted
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullHugeCommitted : public TEventLocal<TEvHullHugeCommitted, TEvBlobStorage::EvHullHugeCommitted> {
    public:
        const ui64 EntryPointLsn;

        TEvHullHugeCommitted(ui64 entryPointLsn)
            : EntryPointLsn(entryPointLsn)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EntryPointLsn# " << EntryPointLsn << "}";
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullHugeWritten
    ////////////////////////////////////////////////////////////////////////////
    class TEvHullHugeWritten : public TEventLocal<TEvHullHugeWritten, TEvBlobStorage::EvHullHugeWritten> {
    public:
        const NHuge::THugeSlot HugeSlot;

        TEvHullHugeWritten(const NHuge::THugeSlot &hugeSlot)
            : HugeSlot(hugeSlot)
        {}

        TString ToString() const {
            return HugeSlot.ToString();
        }
    };



    ////////////////////////////////////////////////////////////////////////////
    // THullHugeBlobWriter
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeBlobWriter : public TActorBootstrapped<THullHugeBlobWriter> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        const TActorId NotifyID;
        const NHuge::THugeSlot HugeSlot;
        std::unique_ptr<TEvHullWriteHugeBlob> Item;
        ui64 WriteId;
        TDiskPart DiskAddr;
        static void *Cookie;
        NWilson::TSpan Span;

        friend class TActorBootstrapped<THullHugeBlobWriter>;

        ui8 GetWritePriority() const {
            switch (Item->HandleClass) {
                // if we got HandleClass=TabletLog, it means that a tablet writes huge record to the log,
                // we treat this blos with high priority, i.e. HullHugeUserData
                case NKikimrBlobStorage::EPutHandleClass::TabletLog:    return NPriWrite::HullHugeUserData;
                case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:    return NPriWrite::HullHugeAsyncBlob;
                case NKikimrBlobStorage::EPutHandleClass::UserData:     return NPriWrite::HullHugeUserData;
                default: Y_FAIL_S("Unexpected HandleClass# " << int(Item->HandleClass));
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            LWTRACK(HugeWriterStart, Item->Orbit);

            // prepare write
            const ui8 partId = Item->LogoBlobId.PartId();
            Y_ABORT_UNLESS(partId != 0);

            const ui32 storedBlobSize = Item->Data.GetSize();
            const ui32 writtenSize = AlignUpAppendBlockSize(storedBlobSize, HugeKeeperCtx->PDiskCtx->Dsk->AppendBlockSize);
            Y_ABORT_UNLESS(writtenSize <= HugeSlot.GetSize());

            NPDisk::TEvChunkWrite::TPartsPtr partsPtr(new NPDisk::TEvChunkWrite::TRopeAlignedParts(std::move(Item->Data), writtenSize));
            ui32 chunkId = HugeSlot.GetChunkId();
            ui32 offset = HugeSlot.GetOffset();
            HugeKeeperCtx->LsmHullGroup.LsmHugeBytesWritten() += partsPtr->ByteSize();
            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                "Writer: bootstrap: id# %s storedBlobSize# %u writtenSize# %u blobId# %s wId# %" PRIu64,
                HugeSlot.ToString().data(), storedBlobSize, writtenSize, Item->LogoBlobId.ToString().data(), WriteId));
            Span && Span.Event("Send_TEvChunkWrite", {{"ChunkId", chunkId}, {"Offset", offset}, {"WrittenSize", writtenSize}});
            auto ev = std::make_unique<NPDisk::TEvChunkWrite>(HugeKeeperCtx->PDiskCtx->Dsk->Owner,
                        HugeKeeperCtx->PDiskCtx->Dsk->OwnerRound, chunkId, offset,
                        partsPtr, Cookie, true, GetWritePriority(), false);
            ev->Orbit = std::move(Item->Orbit);
            ctx.Send(HugeKeeperCtx->PDiskCtx->PDiskId, ev.release(), 0, 0, Span.GetTraceId());
            DiskAddr = TDiskPart(chunkId, offset, storedBlobSize);

            // wait response
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(NPDisk::TEvChunkWriteResult::TPtr &ev, const TActorContext &ctx) {
            LWTRACK(HugeWriterFinish, Item->Orbit, NKikimrProto::EReplyStatus_Name(ev->Get()->Status));
            if (ev->Get()->Status == NKikimrProto::OK) {
                Span.EndOk();
            } else {
                Span.EndError(TStringBuilder() << NKikimrProto::EReplyStatus_Name(ev->Get()->Status));
            }
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            ctx.Send(NotifyID, new TEvHullHugeWritten(HugeSlot));
            ctx.Send(HugeKeeperCtx->SkeletonId, new TEvHullLogHugeBlob(WriteId, Item->LogoBlobId, Item->Ingress, DiskAddr,
                Item->IgnoreBlock, Item->SenderId, Item->Cookie, std::move(Item->Result), &Item->ExtraBlockChecks), 0, 0,
                Span.GetTraceId());
            LOG_DEBUG(ctx, BS_HULLHUGE,
                      VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "Writer: finish: id# %s diskAddr# %s",
                            HugeSlot.ToString().data(), DiskAddr.ToString().data()));
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvChunkWriteResult, Handle)
            CFunc(TEvents::TSystem::Poison, Die)
        )

        void HandlePoison(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
            Span.EndError("EvPoison");
            Die(ctx);
        }
        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_BLOB_WRITER;
        }

        THullHugeBlobWriter(
                std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx,
                const TActorId &notifyID,
                const NHuge::THugeSlot &hugeSlot,
                std::unique_ptr<TEvHullWriteHugeBlob> item,
                ui64 wId,
                NWilson::TTraceId traceId)
            : TActorBootstrapped<TThis>()
            , HugeKeeperCtx(std::move(hugeKeeperCtx))
            , NotifyID(notifyID)
            , HugeSlot(hugeSlot)
            , Item(std::move(item))
            , WriteId(wId)
            , DiskAddr()
            , Span(TWilson::VDiskInternals, std::move(traceId), "VDisk.HugeBlobKeeper.Write")
        {
            if (Span) {
                Span.Attribute("blob_id", Item->LogoBlobId.ToString());
            }
        }
    };

    void *THullHugeBlobWriter::Cookie = (void *)"HugeBlobWriter";

    ////////////////////////////////////////////////////////////////////////////
    // THullHugeBlobChunkAllocator
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeBlobChunkAllocator : public TActorBootstrapped<THullHugeBlobChunkAllocator> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        const TActorId NotifyID;
        ui64 Lsn;
        std::shared_ptr<THullHugeKeeperPersState> Pers;
        ui32 ChunkId = 0;
        NWilson::TSpan Span;
        ui32 SlotSize;

        friend class TActorBootstrapped<THullHugeBlobChunkAllocator>;

        void Bootstrap(const TActorContext &ctx) {
            // reserve chunk
            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkAllocator: bootstrap"));
            ctx.Send(HugeKeeperCtx->PDiskCtx->PDiskId,
                    new NPDisk::TEvChunkReserve(HugeKeeperCtx->PDiskCtx->Dsk->Owner,
                        HugeKeeperCtx->PDiskCtx->Dsk->OwnerRound, 1));
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(NPDisk::TEvChunkReserveResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            Y_ABORT_UNLESS(ev->Get()->ChunkIds.size() == 1);
            ChunkId = ev->Get()->ChunkIds.front();
            Lsn = HugeKeeperCtx->LsnMngr->AllocLsnForLocalUse().Point();

            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkAllocator: reserved:"
                " chunkId# %" PRIu32 " Lsn# %" PRIu64, ChunkId, Lsn));

            // prepare commit record, i.e. commit reserved chunk
            NPDisk::TCommitRecord commitRecord;
            commitRecord.FirstLsnToKeep = 0;
            commitRecord.CommitChunks.push_back(ChunkId);
            commitRecord.IsStartingPoint = false;

            // prepare log record
            NHuge::TAllocChunkRecoveryLogRec logRec(ChunkId);
            TRcBuf data = TRcBuf(logRec.Serialize());

            LOG_INFO(ctx, NKikimrServices::BS_SKELETON,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "huge reserve/commit ChunkIds# %s", FormatList(ev->Get()->ChunkIds).data()));
            LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS,
                      VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "COMMIT: PDiskId# %s Lsn# %" PRIu64 " type# HugeChunkAllocator msg# %s",
                            HugeKeeperCtx->PDiskCtx->PDiskIdString.data(), Lsn, commitRecord.ToString().data()));

            ctx.Send(HugeKeeperCtx->LoggerId, new NPDisk::TEvLog(HugeKeeperCtx->PDiskCtx->Dsk->Owner,
                HugeKeeperCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureHugeBlobAllocChunk,
                commitRecord, data, TLsnSeg(Lsn, Lsn), nullptr));

            // commit changes to the persistent state at once
            const ui64 prevLsn = std::exchange(Pers->LogPos.ChunkAllocationLsn, Lsn);
            Y_ABORT_UNLESS(prevLsn < Lsn);
            Pers->Heap->AddChunk(ChunkId);
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            Y_ABORT_UNLESS(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == Lsn);

            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkAllocator: committed:"
                " chunkId# %" PRIu32 " LsnSeg# %" PRIu64, ChunkId, Lsn));

            ctx.Send(NotifyID, new TEvHullHugeChunkAllocated(ChunkId, SlotSize));
            Die(ctx);
            Span.EndOk();
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvChunkReserveResult, Handle)
            HFunc(NPDisk::TEvLogResult, Handle)
            CFunc(TEvents::TSystem::Poison, Die)
        )

        void HandlePoison(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
            Span.EndError("Poison");
            Die(ctx);
        }

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_BLOB_CHUNKALLOC;
        }

        THullHugeBlobChunkAllocator(std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx, const TActorId &notifyID,
                std::shared_ptr<THullHugeKeeperPersState> pers, NWilson::TTraceId traceId, ui32 slotSize)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
            , NotifyID(notifyID)
            , Pers(std::move(pers))
            , Span(TWilson::VDiskTopLevel, std::move(traceId), "VDisk.HullHugeBlobChunkAllocator")
            , SlotSize(slotSize)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullHugeBlobChunkDestroyer
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeBlobChunkDestroyer : public TActorBootstrapped<THullHugeBlobChunkDestroyer> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        const TActorId NotifyID;
        TVector<ui32> ChunksToFree;
        const ui64 Lsn;

        friend class TActorBootstrapped<THullHugeBlobChunkDestroyer>;

        void Bootstrap(const TActorContext &ctx) {
            // prepare log record
            Y_DEBUG_ABORT_UNLESS(!ChunksToFree.empty());
            NHuge::TFreeChunkRecoveryLogRec logRec(ChunksToFree);
            TRcBuf data = TRcBuf(logRec.Serialize());

            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkDestroyer: bootstrap:"
                " chunks# %s Lsn# %" PRIu64, FormatList(ChunksToFree).data(), Lsn));

            // prepare commit record, i.e. commit reserved chunk
            NPDisk::TCommitRecord commitRecord;
            commitRecord.FirstLsnToKeep = 0;
            Y_DEBUG_ABORT_UNLESS(!ChunksToFree.empty());
            commitRecord.DeleteChunks = ChunksToFree;
            commitRecord.IsStartingPoint = false;

            LOG_INFO(ctx, NKikimrServices::BS_SKELETON,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "huge delete ChunkIds# %s", FormatList(commitRecord.DeleteChunks).data()));
            LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_CHUNKS,
                      VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "COMMIT: PDiskId# %s Lsn# %" PRIu64 " type# HugeChunkDestroyer msg# %s",
                            HugeKeeperCtx->PDiskCtx->PDiskIdString.data(), Lsn, commitRecord.ToString().data()));

            // send log message
            ctx.Send(HugeKeeperCtx->LoggerId, new NPDisk::TEvLog(HugeKeeperCtx->PDiskCtx->Dsk->Owner,
                HugeKeeperCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureHugeBlobFreeChunk,
                commitRecord, data, TLsnSeg(Lsn, Lsn), nullptr));
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            Y_ABORT_UNLESS(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == Lsn);

            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkDestroyer: committed:"
                " chunks# %s Lsn# %" PRIu64, FormatList(ChunksToFree).data(), Lsn));

            ctx.Send(NotifyID, new TEvHullHugeChunkFreed);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvLogResult, Handle)
            CFunc(TEvents::TSystem::Poison, Die)
        )

        void HandlePoison(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) { Die(ctx); }
        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_BLOB_CHUNKDESTROY;
        }

        THullHugeBlobChunkDestroyer(std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx, const TActorId &notifyID,
                TVector<ui32> &&chunksToFree, ui64 lsn)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
            , NotifyID(notifyID)
            , ChunksToFree(std::move(chunksToFree))
            , Lsn(lsn)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullHugeBlobEntryPointSaver
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeBlobEntryPointSaver : public TActorBootstrapped<THullHugeBlobEntryPointSaver> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        const TActorId NotifyID;
        const ui64 EntryPointLsn;
        const TString Serialized;

        friend class TActorBootstrapped<THullHugeBlobEntryPointSaver>;

        void Bootstrap(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE,
                      VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "EntryPointSaver: bootstrap: lsn# %" PRIu64, EntryPointLsn));

            // prepare commit record
            NPDisk::TCommitRecord commitRecord;
            commitRecord.FirstLsnToKeep = 0;
            commitRecord.IsStartingPoint = true; // yes, this is entry point

            // send log message
            TLsnSeg seg(EntryPointLsn, EntryPointLsn);
            ctx.Send(HugeKeeperCtx->LoggerId,
                    new NPDisk::TEvLog(HugeKeeperCtx->PDiskCtx->Dsk->Owner, HugeKeeperCtx->PDiskCtx->Dsk->OwnerRound,
                        TLogSignature::SignatureHugeBlobEntryPoint, commitRecord, TRcBuf(Serialized), seg, nullptr)); //FIXME(innokentii): wrapping
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            Y_ABORT_UNLESS(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == EntryPointLsn);

            LOG_DEBUG(ctx, BS_HULLHUGE,
                      VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "EntryPointSaver: committed: lsn# %" PRIu64, EntryPointLsn));

            ctx.Send(NotifyID, new TEvHullHugeCommitted(EntryPointLsn));
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvLogResult, Handle)
            CFunc(TEvents::TSystem::Poison, Die)
        )

        void HandlePoison(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) { Die(ctx); }
        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_BLOB_ENTRYPOINTSAVER;
        }

        THullHugeBlobEntryPointSaver(std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx, const TActorId &notifyID,
                ui64 entryPointLsn, const TString &serialized)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
            , NotifyID(notifyID)
            , EntryPointLsn(entryPointLsn)
            , Serialized(serialized)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullHugeStatGather
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeStatGather : public TActorBootstrapped<THullHugeStatGather> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        const TActorId ParentId;
        // we update stat every RenewPeriod seconds
        const TDuration RenewPeriod = TDuration::Seconds(15);

        friend class TActorBootstrapped<THullHugeStatGather>;

        void Bootstrap(const TActorContext &ctx) {
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup());
            TThis::Become(&TThis::StateWaitTimeout);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            ctx.Send(ParentId, new TEvHugeStat());
            TThis::Become(&TThis::StateWaitStat);
        }

        void Handle(TEvHugeStatResult::TPtr &ev, const TActorContext &ctx) {
            // We don't need result, we trigger huge stat calculation, the keeper
            // updates global state in VCtx
            Y_UNUSED(ev);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup());
            TThis::Become(&TThis::StateWaitTimeout);
        }

        STRICT_STFUNC(StateWaitTimeout,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )

        STRICT_STFUNC(StateWaitStat,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvHugeStatResult, Handle);
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_KEEPER;
        }

        THullHugeStatGather(std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx, const TActorId &parentId)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
            , ParentId(parentId)
        {}
    };
    ////////////////////////////////////////////////////////////////////////////
    // THullHugeKeeperState
    ////////////////////////////////////////////////////////////////////////////
    struct THullHugeKeeperState {
        THashMap<ui32, std::deque<NWilson::TWithSpan<std::unique_ptr<TEvHullWriteHugeBlob::THandle>>>> WaitQueue;

        bool Committing = false;
        ui64 FreeUpToLsn = 0;           // last value we got from PDisk
        TMaybe<TInstant> LastCommitTime;
        std::shared_ptr<THullHugeKeeperPersState> Pers;
        std::map<ui64, std::unique_ptr<IEventHandle>> PendingWrites;
        size_t MaxPendingWrites = 0;
        bool ProcessingPendingWrite = false;
        THugeBlobLogLsnFifo LsnFifo{1};
        ui64 LastReportedFirstLsnToKeep = 0;
        ui32 ItemsAfterCommit = 0;

        THullHugeKeeperState(std::shared_ptr<THullHugeKeeperPersState> &&pers)
            : Pers(std::move(pers))
        {}

        ui64 FirstLsnToKeep() const {
            const ui64 pendingLsn = PendingWrites.empty() ? Max<ui64>() : PendingWrites.begin()->first;
            return Pers->FirstLsnToKeep(Min(pendingLsn, LsnFifo.FirstLsnToKeep()));
        }

        TString FirstLsnToKeepDecomposed() const {
            TStringStream str;
            str << "{FirstLsnToKeep# " << FirstLsnToKeep()
                << " pers# " << Pers->FirstLsnToKeepDecomposed()
                << " LsnFifo# " << LsnFifo.FirstLsnToKeepDecomposed()
                << "}";
            return str.Str();
        }

        void RenderHtml(IOutputStream &str) {
            auto boolToString = [] (bool x) { return x ? "true" : "false"; };
            str << "<pre>";
            str << "Committing:       " << boolToString(Committing) << Endl;
            str << "ItemsAfterCommit: " << ItemsAfterCommit << Endl;
            str << "FreeUpToLsn:      " << FreeUpToLsn << Endl;
            str << "LastCommitTime:   " << (LastCommitTime ? ToStringLocalTimeUpToSeconds(*LastCommitTime) : "not yet") << Endl;
            str << "FirstLsnToKeep:   " << FirstLsnToKeep() << Endl;
            str << "PendingWrites:    " << PendingWrites.size() << '/' << MaxPendingWrites << " max" << Endl;
            str << "LsnFifo: " << LsnFifo.ToString() << Endl;
            str << "</pre>";
            Pers->RenderHtml(str);
        }

        void Output(IOutputStream &str) const {
            auto boolToString = [] (bool x) { return x ? "true" : "false"; };
            str << "{Committing# " << boolToString(Committing)
                << " FreeUpToLsn# " << FreeUpToLsn
                << " LastCommitTime# " << (LastCommitTime ? ToStringLocalTimeUpToSeconds(*LastCommitTime) : "not yet")
                << " FirstLsnToKeep# " << FirstLsnToKeep()
                << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeKeeperCtx
    ////////////////////////////////////////////////////////////////////////////
    THugeKeeperCtx::THugeKeeperCtx(
            TIntrusivePtr<TVDiskContext> vctx,
            TPDiskCtxPtr pdiskCtx,
            TIntrusivePtr<TLsnMngr> lsnMngr,
            TActorId skeletonId,
            TActorId loggerId,
            TActorId logCutterId,
            const TString &localRecoveryInfoDbg,
            bool isReadOnlyVDisk)
        : VCtx(std::move(vctx))
        , PDiskCtx(std::move(pdiskCtx))
        , LsnMngr(std::move(lsnMngr))
        , SkeletonId(skeletonId)
        , LoggerId(loggerId)
        , LogCutterId(logCutterId)
        , LocalRecoveryInfoDbg(localRecoveryInfoDbg)
        , LsmHullGroup(VCtx->VDiskCounters, "subsystem", "lsmhull")
        , DskOutOfSpaceGroup(VCtx->VDiskCounters, "subsystem", "outofspace")
        , IsReadOnlyVDisk(isReadOnlyVDisk)
    {}

    THugeKeeperCtx::~THugeKeeperCtx() = default;


    ////////////////////////////////////////////////////////////////////////////
    // THullHugeKeeper
    ////////////////////////////////////////////////////////////////////////////
    class THullHugeKeeper : public TActorBootstrapped<THullHugeKeeper> {
        std::shared_ptr<THugeKeeperCtx> HugeKeeperCtx;
        THullHugeKeeperState State;
        TActiveActors ActiveActors;
        std::unordered_set<ui32> AllocatingChunkPerSlotSize;

        void CheckLsn(ui64 lsn, const char *action) {
            const ui64 firstLsnToKeep = State.FirstLsnToKeep();
            Y_VERIFY_S(firstLsnToKeep <= lsn, HugeKeeperCtx->VCtx->VDiskLogPrefix
                << "FirstLsnToKeep# " << firstLsnToKeep
                << " Lsn# " << lsn
                << " Action# " << action
                << " LsnFifo# " << State.LsnFifo.ToString());
        }

        void PutToWaitQueue(ui32 slotSize, std::unique_ptr<TEvHullWriteHugeBlob::THandle> item) {
            State.WaitQueue[slotSize].emplace_back(std::move(item), NWilson::TSpan(TWilson::VDiskTopLevel,
                std::move(item->TraceId), "VDisk.HullHugeKeeper.InWaitQueue"));
        }

        bool ProcessWrite(std::unique_ptr<TEvHullWriteHugeBlob::THandle>& ev, const TActorContext& ctx,
                NWilson::TTraceId traceId, bool putToQueue) {
            auto& msg = *ev->Get();
            NHuge::THugeSlot hugeSlot;
            ui32 slotSize;
            if (State.Pers->Heap->Allocate(msg.Data.GetSize(), &hugeSlot, &slotSize)) {
                const bool inserted = State.Pers->AllocatedSlots.insert(hugeSlot).second;
                Y_ABORT_UNLESS(inserted);
                const ui64 lsnInfimum = HugeKeeperCtx->LsnMngr->GetLsn();
                CheckLsn(lsnInfimum, "WriteHugeBlob");
                const ui64 wId = State.LsnFifo.Push(lsnInfimum);
                auto aid = ctx.Register(new THullHugeBlobWriter(HugeKeeperCtx, ctx.SelfID, hugeSlot,
                    std::unique_ptr<TEvHullWriteHugeBlob>(ev->Release().Release()), wId, std::move(traceId)));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                return true;
            } else if (AllocatingChunkPerSlotSize.insert(slotSize).second) {
                LWTRACK(HugeBlobChunkAllocatorStart, ev->Get()->Orbit);
                auto aid = ctx.RegisterWithSameMailbox(new THullHugeBlobChunkAllocator(HugeKeeperCtx, ctx.SelfID,
                    State.Pers, std::move(traceId), slotSize));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
            if (putToQueue) {
                PutToWaitQueue(slotSize, std::move(ev));
            }
            return false;
        }

        void ProcessQueue(ui32 slotSize, const TActorContext &ctx) {
            auto& queue = State.WaitQueue[slotSize];
            auto it = queue.begin();
            while (it != queue.end() && ProcessWrite(it->Item, ctx, it->Span.GetTraceId(), false)) {
                it->Span.EndOk();
                ++it;
            }
            queue.erase(queue.begin(), it);
        }

        void FreeChunks(const TActorContext &ctx) {
            TVector<ui32> vec;
            while (ui32 chunkId = State.Pers->Heap->RemoveChunk()) {
                vec.push_back(chunkId);
            }
            if (!vec.empty()) {
                const ui64 lsn = HugeKeeperCtx->LsnMngr->AllocLsnForLocalUse().Point();
                auto aid = ctx.Register(new THullHugeBlobChunkDestroyer(HugeKeeperCtx, ctx.SelfID, std::move(vec), lsn));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                const ui64 prevLsn = std::exchange(State.Pers->LogPos.ChunkFreeingLsn, lsn);
                Y_ABORT_UNLESS(prevLsn < lsn); // although it is useless :)
            }
        }

        //////////// Cut Log Handler ///////////////////////////////////
        void TryToCutLog(const TActorContext &ctx) {
            if (HugeKeeperCtx->IsReadOnlyVDisk) {
                LOG_DEBUG(ctx, BS_LOGCUTTER,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TryToCutLog: terminate; readonly vdisk"));
                return;
            }
            const ui64 firstLsnToKeep = State.FirstLsnToKeep();
            LOG_DEBUG(ctx, BS_LOGCUTTER,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                    "THullHugeKeeper: TryToCutLog: state# %s firstLsnToKeep# %" PRIu64
                    " FirstLsnToKeepDecomposed# %s", State.ToString().data(), firstLsnToKeep,
                    State.FirstLsnToKeepDecomposed().data()));

            // notify log cutter if the FirstLsnToKeep has changed since last reporting
            if (firstLsnToKeep != State.LastReportedFirstLsnToKeep) {
                Y_VERIFY_S(firstLsnToKeep > State.LastReportedFirstLsnToKeep, "huge keeper log rollback"
                        << " firstLsnToKeep#" << firstLsnToKeep
                        << " State.LastReportedFirstLsnToKeep# " << State.LastReportedFirstLsnToKeep);
                ctx.Send(HugeKeeperCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::HugeKeeper, firstLsnToKeep));
                State.LastReportedFirstLsnToKeep = firstLsnToKeep;

                LOG_DEBUG(ctx, BS_LOGCUTTER,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TryToCutLog: send TEvVDiskCutLog; firstLsnToKeep# %" PRIu64,
                        firstLsnToKeep));
            }

            // do nothing if commit is in progress
            if (State.Committing) {
                LOG_DEBUG(ctx, BS_LOGCUTTER,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "THullHugeKeeper: TryToCutLog: terminate 0; state# %s", State.ToString().data()));
                return;
            }

            // check what if we issue a new huge hull keeper entry point -- would it allow us to
            // move the FirstLsnToKeep barrier forward? if so, try to issue an entry point, otherwise exit
            const ui64 pendingLsn = State.PendingWrites.empty() ? Max<ui64>() : State.PendingWrites.begin()->first;
            const ui64 minInFlightLsn = Min(pendingLsn, State.LsnFifo.FirstLsnToKeep());
            if (!State.Pers->WouldNewEntryPointAdvanceLog(State.FreeUpToLsn, minInFlightLsn, State.ItemsAfterCommit)) {
                // if we issue an entry point now, we will achieve nothing, so return
                LOG_DEBUG(ctx, BS_LOGCUTTER,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "THullHugeKeeper: TryToCutLog: terminate 1; state# %s", State.ToString().data()));
                return;
            }


            // allocate LSN for the brand new entry point
            ui64 lsn = HugeKeeperCtx->LsnMngr->AllocLsnForLocalUse().Point();
            State.Pers->InitiateNewEntryPointCommit(lsn, minInFlightLsn);
            State.Committing = true;
            State.ItemsAfterCommit = 0;
            // serialize log record into string
            TString serialized = State.Pers->Serialize();

            // run committer
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "THullHugeKeeper: Commit initiated"));
            LOG_DEBUG(ctx, BS_LOGCUTTER,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "THullHugeKeeper: TryToCutLog: run committer"));

            auto aid = ctx.Register(new THullHugeBlobEntryPointSaver(HugeKeeperCtx, ctx.SelfID, lsn, serialized));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }
        //////////// Cut Log Handler ///////////////////////////////////

        template<typename TEvent>
        bool CheckPendingWrite(ui64 writeId, TAutoPtr<TEventHandle<TEvent>>& ev, ui64 lsn, const char *action) {
            LOG_DEBUG_S(*TlsActivationContext, BS_HULLHUGE, HugeKeeperCtx->VCtx->VDiskLogPrefix << "CheckPendingWrite"
                << " WriteId# " << writeId
                << " ProcessingPendingWrite# " << State.ProcessingPendingWrite
                << " Lsn# " << lsn
                << " Action# " << action
                << " LsnFifo# " << State.LsnFifo.ToString()
                << " PendingWrites.size# " << State.PendingWrites.size()
                << " FirstPendingWrite# " << (State.PendingWrites.empty() ? 0 : State.PendingWrites.begin()->first));

            Y_ABORT_UNLESS(writeId);
            if (State.ProcessingPendingWrite) {
                return false;
            }

            CheckLsn(lsn, action);
            State.LsnFifo.Pop(writeId, lsn, true);
            const bool canProcessNow = State.PendingWrites.empty() && lsn < State.LsnFifo.FirstLsnToKeep();
            if (canProcessNow) {
                return false;
            } else {
                const auto [it, inserted] = State.PendingWrites.emplace(lsn, ev.Release());
                Y_ABORT_UNLESS(inserted);
                State.MaxPendingWrites = Max(State.MaxPendingWrites, State.PendingWrites.size());
                ProcessPendingWrites();
                return true;
            }
        }

        void ProcessPendingWrites() {
            State.ProcessingPendingWrite = true;
            auto it = State.PendingWrites.begin();
            while (it != State.PendingWrites.end()) {
                auto& [lsn, ev] = *it;
                if (lsn < State.LsnFifo.FirstLsnToKeep()) {
                    TAutoPtr<IEventHandle> ptr(ev.release());
                    Receive(ptr);
                    ++it;
                } else {
                    break;
                }
            }
            State.PendingWrites.erase(State.PendingWrites.begin(), it);
            State.ProcessingPendingWrite = false;
        }

        //////////// Event Handlers ////////////////////////////////////
        void Handle(TEvHullWriteHugeBlob::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                "THullHugeKeeper: TEvHullWriteHugeBlob: %s", std::data(ev->Get()->ToString())));
            LWTRACK(HugeKeeperWriteHugeBlobReceived, ev->Get()->Orbit);
            std::unique_ptr<TEvHullWriteHugeBlob::THandle> item(ev.Release());
            ProcessWrite(item, ctx, item->TraceId.Clone(), true);
        }

        void Handle(TEvHullHugeChunkAllocated::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "THullHugeKeeper:"
                " TEvHullHugeChunkAllocated: %s", msg->ToString().data()));
            const size_t numErased = AllocatingChunkPerSlotSize.erase(msg->SlotSize);
            Y_ABORT_UNLESS(numErased == 1);
            ActiveActors.Erase(ev->Sender);
            ProcessQueue(msg->SlotSize, ctx);
        }

        void Handle(TEvHullFreeHugeSlots::TPtr &ev, const TActorContext &ctx) {
            TEvHullFreeHugeSlots *msg = ev->Get();
            Y_ABORT_UNLESS(!msg->HugeBlobs.Empty());

            if (CheckPendingWrite(msg->WId, ev, msg->DeletionLsn, "FreeHugeSlots")) {
                return;
            }

            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "THullHugeKeeper: TEvHullFreeHugeSlots: %s", msg->ToString().data()));

            THashSet<ui32> slotSizes;
            for (const auto &x : msg->HugeBlobs) {
                slotSizes.insert(State.Pers->Heap->SlotSizeOfThisSize(x.Size));
                NHuge::TFreeRes freeRes = State.Pers->Heap->Free(x);
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                                "THullHugeKeeper: TEvHullFreeHugeSlots: one slot: addr# %s",
                                x.ToString().data()));
                ++State.ItemsAfterCommit;
            }

            auto checkAndSet = [this, msg] (ui64 &dbLsn) {
                ui64 origRecoveredLsn = HugeKeeperCtx->LsnMngr->GetOriginallyRecoveredLsn();
                Y_VERIFY_S(dbLsn <= msg->DeletionLsn, HugeKeeperCtx->VCtx->VDiskLogPrefix << " Check failed:"
                        << " dbLsn# " << dbLsn << " origRecoveredLsn# " << origRecoveredLsn
                        << " recovInfo# " << HugeKeeperCtx->LocalRecoveryInfoDbg
                        << " msg# " << msg->ToString());
                dbLsn = msg->DeletionLsn;
            };

            switch (msg->Signature) {
                case TLogSignature::SignatureHullLogoBlobsDB:
                    checkAndSet(State.Pers->LogPos.LogoBlobsDbSlotDelLsn);
                    break;
                case TLogSignature::SignatureHullBlocksDB:
                case TLogSignature::SignatureHullBarriersDB:
                default:
                    Y_ABORT("Impossible case");
            }
            for (ui32 slotSize : slotSizes) {
                ProcessQueue(slotSize, ctx);
            }
            FreeChunks(ctx);
        }

        void Handle(TEvHullHugeChunkFreed::TPtr& ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "THullHugeKeeper: TEvHullHugeChunkFreed"));
            ActiveActors.Erase(ev->Sender);
        }

        void Handle(TEvHullHugeCommitted::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvHullHugeCommitted: %s", ev->Get()->ToString().data()));
            Y_ABORT_UNLESS(State.Committing);
            State.Committing = false;
            ActiveActors.Erase(ev->Sender);
            State.LastCommitTime = TAppData::TimeProvider->Now();
            State.Pers->EntryPointCommitted(ev->Get()->EntryPointLsn);
        }

        void Handle(TEvHullHugeWritten::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvHullHugeWritten: %s", ev->Get()->ToString().data()));
            ActiveActors.Erase(ev->Sender);
        }

        void Handle(TEvHullHugeBlobLogged::TPtr &ev, const TActorContext &ctx) {
            const TEvHullHugeBlobLogged *msg = ev->Get();

            if (!msg->SlotIsUsed) {
                State.LsnFifo.Pop(msg->WriteId, msg->RecLsn, false);
                ProcessPendingWrites();
            } else if (CheckPendingWrite(msg->WriteId, ev, msg->RecLsn, "HugeBlobLogged")) {
                return;
            }

            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvHullHugeBlobLogged: %s", msg->ToString().data()));
            // manage log requests in flight
            State.ItemsAfterCommit += msg->SlotIsUsed;
            // manage allocated slots
            const TDiskPart &hugeBlob = msg->HugeBlob;
            NHuge::THugeSlot hugeSlot(State.Pers->Heap->ConvertDiskPartToHugeSlot(hugeBlob));
            auto nErased = State.Pers->AllocatedSlots.erase(hugeSlot);
            Y_ABORT_UNLESS(nErased == 1);
            // depending on SlotIsUsed...
            if (msg->SlotIsUsed) {
                Y_VERIFY_S(State.Pers->LogPos.HugeBlobLoggedLsn < msg->RecLsn,
                        "pers# " << State.Pers->ToString() << " msg# " << msg->ToString());
                // ...update HugeBlobLoggedLsn (monotonically incremented)
                State.Pers->LogPos.HugeBlobLoggedLsn = msg->RecLsn;
            } else {
                // ...free slot
                State.Pers->Heap->Free(hugeBlob);
            }
        }

        void Handle(TEvHugePreCompact::TPtr ev, const TActorContext& ctx) {
            const ui64 lsnInfimum = HugeKeeperCtx->LsnMngr->GetLsn();
            CheckLsn(lsnInfimum, "PreCompact");
            const ui64 wId = State.LsnFifo.Push(lsnInfimum);
            LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLHUGE, HugeKeeperCtx->VCtx->VDiskLogPrefix
                << "THullHugeKeeper: requested PreCompact wId# " << wId
                << " LsnInfimum# " << lsnInfimum);
            ctx.Send(ev->Sender, new TEvHugePreCompactResult(wId), 0, ev->Cookie);
        }

        void Handle(TEvHugeLockChunks::TPtr &ev, const TActorContext &ctx) {
            const TEvHugeLockChunks *msg = ev->Get();
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvHugeLockChunks: %s", msg->ToString().data()));

            TDefragChunks lockedChunks;
            lockedChunks.reserve(msg->Chunks.size());
            for (const auto &d : msg->Chunks) {
                bool locked = State.Pers->Heap->LockChunkForAllocation(d.ChunkId, d.SlotSize);
                if (locked) {
                    lockedChunks.emplace_back(d.ChunkId, d.SlotSize);
                }
            }
            ctx.Send(ev->Sender, new TEvHugeLockChunksResult(std::move(lockedChunks)));
        }

        void Handle(TEvHugeUnlockChunks::TPtr& ev, const TActorContext& /*ctx*/) {
            for (const auto& d : ev->Get()->Chunks) {
                State.Pers->Heap->UnlockChunk(d.ChunkId, d.SlotSize);
            }
        }

        void Handle(TEvHugeStat::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_HULLHUGE,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvHugeStat"));

            auto res = std::make_unique<TEvHugeStatResult>();
            res->Stat = State.Pers->Heap->GetStat();
            UpdateGlobalFragmentationStat(res->Stat);
            ctx.Send(ev->Sender, res.release());
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_LOGCUTTER,
                VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                        "THullHugeKeeper: TEvCutLog: %s", ev->Get()->ToString().data()));
            State.FreeUpToLsn = ev->Get()->FreeUpToLsn;
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::HugeKeeperId);
            TStringStream str;
            HTML(str) {
                DIV_CLASS("panel panel-default") {
                    DIV_CLASS("panel-heading") {str << "Huge Blob Keeper";}
                    DIV_CLASS("panel-body") {State.RenderHtml(str);}
                }
            }
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::HugeKeeperId));
        }


        void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        void UpdateGlobalFragmentationStat(const THeapStat &stat) {
            // update mon counters
            HugeKeeperCtx->DskOutOfSpaceGroup.HugeUsedChunks() = stat.CurrentlyUsedChunks;
            HugeKeeperCtx->DskOutOfSpaceGroup.HugeCanBeFreedChunks() = stat.CanBeFreedChunks;
            HugeKeeperCtx->DskOutOfSpaceGroup.HugeLockedChunks() = stat.LockedChunks.size();
            // update global stat
            HugeKeeperCtx->VCtx->GetHugeHeapFragmentation().Set(stat.CurrentlyUsedChunks, stat.CanBeFreedChunks);
        }

        //////////// Event Handlers ////////////////////////////////////

        STFUNC(StateFunc) {
            const bool poison = ev->GetTypeRewrite() == TEvents::TSystem::Poison;
            STRICT_STFUNC_BODY(
                HFunc(TEvHullWriteHugeBlob, Handle)
                HFunc(TEvHullHugeChunkAllocated, Handle)
                HFunc(TEvHullFreeHugeSlots, Handle)
                HFunc(TEvHullHugeChunkFreed, Handle)
                HFunc(TEvHullHugeCommitted, Handle)
                HFunc(TEvHullHugeWritten, Handle)
                HFunc(TEvHullHugeBlobLogged, Handle)
                HFunc(TEvHugePreCompact, Handle)
                HFunc(TEvHugeLockChunks, Handle)
                HFunc(TEvHugeUnlockChunks, Handle)
                HFunc(TEvHugeStat, Handle)
                HFunc(NPDisk::TEvCutLog, Handle)
                HFunc(NMon::TEvHttpInfo, Handle)
                HFunc(TEvents::TEvPoisonPill, Handle)
            )
            if (!poison) {
                TryToCutLog(TActivationContext::AsActorContext());
            }
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_HUGE_KEEPER;
        }

        THullHugeKeeper(
                std::shared_ptr<THugeKeeperCtx> &&hugeKeeperCtx,
                std::shared_ptr<THullHugeKeeperPersState> &&persState)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
            , State(std::move(persState))
        {
            Y_ABORT_UNLESS(State.Pers->Recovered &&
                     State.Pers->AllocatedSlots.empty());
        }

        void Bootstrap(const TActorContext &ctx) {
            // update global fragmentation stat
            UpdateGlobalFragmentationStat(State.Pers->Heap->GetStat());
            // run actor that periodically gather huge stat
            auto aid = ctx.Register(new THullHugeStatGather(HugeKeeperCtx, SelfId()));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            // issue entrypoint just at the start
            TryToCutLog(ctx);
            Become(&TThis::StateFunc);
        }
    };

    IActor *CreateHullHugeBlobKeeper(
            std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx,
            std::shared_ptr<THullHugeKeeperPersState> persState) {
        return new THullHugeKeeper(std::move(hugeKeeperCtx), std::move(persState));
    }

} // NKikimr
