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
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
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

        ui64 Push(const TString& prefix, ui64 lsn) {
            Y_VERIFY_S(Fifo.empty() || (--Fifo.end())->second <= lsn, prefix);
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

        void Pop(const TString& prefix, ui64 wId, ui64 lsn, bool logged) {
            const auto it = Fifo.find(wId);
            Y_VERIFY_S(it != Fifo.end(), prefix);
            Y_VERIFY_S(!logged || it->second <= lsn, prefix);
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
            Y_VERIFY_S(partId != 0, HugeKeeperCtx->VCtx->VDiskLogPrefix);

            const ui32 storedBlobSize = Item->Data.GetSize();
            const ui32 writtenSize = AlignUpAppendBlockSize(storedBlobSize, HugeKeeperCtx->PDiskCtx->Dsk->AppendBlockSize);
            Y_VERIFY_S(writtenSize <= HugeSlot.GetSize(), HugeKeeperCtx->VCtx->VDiskLogPrefix);

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
                Item->IgnoreBlock, Item->IssueKeepFlag, Item->SenderId, Item->Cookie, Item->HandleClass,
                std::move(Item->Result), &Item->ExtraBlockChecks, Item->RewriteBlob), 0, 0, Span.GetTraceId());
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
        TActorId ParentId;
        ui64 Lsn;
        std::shared_ptr<THullHugeKeeperPersState> Pers;
        ui32 ChunkId = 0;
        NWilson::TSpan Span;
        ui32 SlotSize;

        friend class TActorBootstrapped<THullHugeBlobChunkAllocator>;

        void Bootstrap(TActorId parentId, const TActorContext &ctx) {
            ParentId = parentId;
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
            Y_VERIFY_S(ev->Get()->ChunkIds.size() == 1, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            ChunkId = ev->Get()->ChunkIds.front();
            Lsn = HugeKeeperCtx->LsnMngr->AllocLsnForLocalUse().Point();
            ctx.Send(HugeKeeperCtx->SkeletonId, new TEvNotifyChunksDeleted(Lsn, ev->Get()->ChunkIds));

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
            Y_VERIFY_S(prevLsn < Lsn, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            Pers->Heap->AddChunk(ChunkId);
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(HugeKeeperCtx->VCtx, ev, ctx);
            Y_VERIFY_S(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == Lsn,
                HugeKeeperCtx->VCtx->VDiskLogPrefix);

            LOG_DEBUG(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkAllocator: committed:"
                " chunkId# %" PRIu32 " LsnSeg# %" PRIu64, ChunkId, Lsn));

            ctx.Send(ParentId, new TEvHullHugeChunkAllocated(ChunkId, SlotSize));
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

        THullHugeBlobChunkAllocator(std::shared_ptr<THugeKeeperCtx> hugeKeeperCtx,
                std::shared_ptr<THullHugeKeeperPersState> pers, NWilson::TTraceId traceId, ui32 slotSize)
            : HugeKeeperCtx(std::move(hugeKeeperCtx))
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
            Y_VERIFY_S(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == Lsn,
                HugeKeeperCtx->VCtx->VDiskLogPrefix);

            LOG_INFO(ctx, BS_HULLHUGE, VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix, "ChunkDestroyer: committed:"
                " chunks# %s Lsn# %" PRIu64, FormatList(ChunksToFree).data(), Lsn));

            ctx.Send(HugeKeeperCtx->SkeletonId, new TEvNotifyChunksDeleted(Lsn, ChunksToFree));
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
            Y_VERIFY_S(ev->Get()->Results.size() == 1 && ev->Get()->Results.front().Lsn == EntryPointLsn,
                HugeKeeperCtx->VCtx->VDiskLogPrefix);

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
        std::multimap<ui64, std::unique_ptr<IEventHandle>> PendingLockResponses;
        std::set<ui64> WritesInFlight;

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
                State.Pers->AddSlotInFlight(hugeSlot);
                State.Pers->AddChunkSize(hugeSlot);
                const ui64 lsnInfimum = HugeKeeperCtx->LsnMngr->GetLsn();
                CheckLsn(lsnInfimum, "WriteHugeBlob");
                const ui64 wId = State.LsnFifo.Push(HugeKeeperCtx->VCtx->VDiskLogPrefix, lsnInfimum);
                WritesInFlight.insert(wId);
                auto aid = ctx.Register(new THullHugeBlobWriter(HugeKeeperCtx, ctx.SelfID, hugeSlot,
                    std::unique_ptr<TEvHullWriteHugeBlob>(ev->Release().Release()), wId, std::move(traceId)));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                return true;
            } else if (AllocatingChunkPerSlotSize.insert(slotSize).second) {
                LWTRACK(HugeBlobChunkAllocatorStart, ev->Get()->Orbit);
                auto aid = ctx.RegisterWithSameMailbox(new THullHugeBlobChunkAllocator(HugeKeeperCtx, State.Pers,
                    std::move(traceId), slotSize));
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
                Y_VERIFY_S(prevLsn < lsn, HugeKeeperCtx->VCtx->VDiskLogPrefix); // although it is useless :)
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
                Y_VERIFY_S(firstLsnToKeep > State.LastReportedFirstLsnToKeep,
                        HugeKeeperCtx->VCtx->VDiskLogPrefix << "huge keeper log rollback"
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

            Y_VERIFY_S(writeId, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            if (State.ProcessingPendingWrite) {
                return false;
            }

            CheckLsn(lsn, action);
            State.LsnFifo.Pop(HugeKeeperCtx->VCtx->VDiskLogPrefix, writeId, lsn, true);
            const bool canProcessNow = State.PendingWrites.empty() && lsn < State.LsnFifo.FirstLsnToKeep();
            if (canProcessNow) {
                return false;
            } else {
                const auto [it, inserted] = State.PendingWrites.emplace(lsn, ev.Release());
                Y_VERIFY_S(inserted, HugeKeeperCtx->VCtx->VDiskLogPrefix);
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
            Y_VERIFY_S(numErased == 1, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            ActiveActors.Erase(ev->Sender);
            ProcessAllocateSlotTasks(msg->SlotSize, ctx);
            ProcessQueue(msg->SlotSize, ctx);
        }

        void Handle(TEvHullFreeHugeSlots::TPtr &ev, const TActorContext &ctx) {
            TEvHullFreeHugeSlots *msg = ev->Get();

            if (msg->WId && CheckPendingWrite(msg->WId, ev, msg->DeletionLsn, "FreeHugeSlots")) {
                return;
            }

            LOG_DEBUG(ctx, BS_HULLHUGE,
                    VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                            "THullHugeKeeper: TEvHullFreeHugeSlots: %s", msg->ToString().data()));

            THashSet<ui32> slotSizes;

            for (const auto &x : msg->HugeBlobs) {
                slotSizes.insert(State.Pers->Heap->SlotSizeOfThisSize(x.Size));
                State.Pers->Heap->Free(x);
                State.Pers->DeleteChunkSize(State.Pers->Heap->ConvertDiskPartToHugeSlot(x));
                LOG_DEBUG(ctx, BS_HULLHUGE,
                          VDISKP(HugeKeeperCtx->VCtx->VDiskLogPrefix,
                                "THullHugeKeeper: TEvHullFreeHugeSlots: one slot: addr# %s",
                                x.ToString().data()));
                ++State.ItemsAfterCommit;
            }

            for (const TDiskPart& x : msg->AllocatedBlobs) {
                const bool deleted = State.Pers->DeleteSlotInFlight(State.Pers->Heap->ConvertDiskPartToHugeSlot(x));
                Y_VERIFY_S(deleted, HugeKeeperCtx->VCtx->VDiskLogPrefix);
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
            Y_VERIFY_S(State.Committing, HugeKeeperCtx->VCtx->VDiskLogPrefix);
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
                State.LsnFifo.Pop(HugeKeeperCtx->VCtx->VDiskLogPrefix, msg->WriteId, msg->RecLsn, false);
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
            const bool deleted = State.Pers->DeleteSlotInFlight(hugeSlot);
            Y_VERIFY_S(deleted, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            // depending on SlotIsUsed...
            if (msg->SlotIsUsed) {
                Y_VERIFY_S(State.Pers->LogPos.HugeBlobLoggedLsn < msg->RecLsn, HugeKeeperCtx->VCtx->VDiskLogPrefix <<
                        "pers# " << State.Pers->ToString() << " msg# " << msg->ToString());
                // ...update HugeBlobLoggedLsn (monotonically incremented)
                State.Pers->LogPos.HugeBlobLoggedLsn = msg->RecLsn;
            } else {
                // ...free slot
                State.Pers->Heap->Free(hugeBlob);
                // and remove chunk size record
                State.Pers->DeleteChunkSize(hugeSlot);
            }

            size_t numErased = WritesInFlight.erase(msg->WriteId);
            Y_VERIFY_S(numErased, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            CheckPendingLockResponses();
        }

        void Handle(TEvHugePreCompact::TPtr ev, const TActorContext& ctx) {
            const ui64 lsnInfimum = HugeKeeperCtx->LsnMngr->GetLsn();
            CheckLsn(lsnInfimum, "PreCompact");
            const ui64 wId = State.LsnFifo.Push(HugeKeeperCtx->VCtx->VDiskLogPrefix, lsnInfimum);
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
                    lockedChunks.push_back(d);
                }
            }

            auto response = std::make_unique<TEvHugeLockChunksResult>(std::move(lockedChunks));
            auto handle = std::make_unique<IEventHandle>(ev->Sender, SelfId(), response.release());
            if (WritesInFlight.empty()) {
                TActivationContext::Send(handle.release());
            } else {
                PendingLockResponses.emplace(*--WritesInFlight.end(), std::move(handle));
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

        void Handle(TEvHugeShredNotify::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            std::ranges::sort(msg->ChunksToShred);
            State.Pers->Heap->ShredNotify(msg->ChunksToShred);
            FreeChunks(ctx);

            auto handle = std::make_unique<IEventHandle>(TEvBlobStorage::EvHugeShredNotifyResult, 0, ev->Sender, SelfId(),
                nullptr, 0);
            if (WritesInFlight.empty()) {
                TActivationContext::Send(handle.release());
            } else {
                PendingLockResponses.emplace(*--WritesInFlight.end(), std::move(handle));
            }
        }

        void Handle(TEvListChunks::TPtr ev, const TActorContext& ctx) {
            auto response = std::make_unique<TEvListChunksResult>();
            State.Pers->Heap->ListChunks(ev->Get()->ChunksOfInterest, response->ChunksHuge);
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void HandleQueryForbiddenChunks(TAutoPtr<IEventHandle> ev, const TActorContext& ctx) {
            ctx.Send(ev->Sender, new TEvHugeForbiddenChunks(State.Pers->Heap->GetForbiddenChunks()), 0, ev->Cookie);
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

        void CheckPendingLockResponses() {
            const ui64 writeId = WritesInFlight.empty() ? Max<ui64>() : *WritesInFlight.begin();
            std::multimap<ui64, std::unique_ptr<IEventHandle>>::iterator it;
            for (it = PendingLockResponses.begin(); it != PendingLockResponses.end() && it->first < writeId; ++it) {
                TActivationContext::Send(it->second.release());
            }
            PendingLockResponses.erase(PendingLockResponses.begin(), it);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TAllocateSlotsTask {
            TActorId Sender;
            ui64 Cookie;
            std::vector<ui32> BlobSizes;
            std::vector<TDiskPart> Result;
            THashMap<ui32, TDynBitMap> Pending;

            TAllocateSlotsTask(TEvHugeAllocateSlots::TPtr& ev)
                : Sender(ev->Sender)
                , Cookie(ev->Cookie)
                , BlobSizes(std::move(ev->Get()->BlobSizes))
                , Result(BlobSizes.size())
            {}
        };

        std::set<std::tuple<ui32, std::shared_ptr<TAllocateSlotsTask>>> SlotSizeToTask;

        void TryToFulfillTask(const std::shared_ptr<TAllocateSlotsTask>& task, ui32 slotSizeToProcess, const TActorContext& ctx) {
            bool done = true;

            auto processItem = [&](size_t index, TDynBitMap *pending) {
                auto& result = task->Result[index];
                Y_DEBUG_ABORT_UNLESS(result.Empty());

                NHuge::THugeSlot hugeSlot;
                ui32 slotSize;
                if (State.Pers->Heap->Allocate(task->BlobSizes[index], &hugeSlot, &slotSize)) {
                    State.Pers->AddSlotInFlight(hugeSlot);
                    State.Pers->AddChunkSize(hugeSlot);
                    result = hugeSlot.GetDiskPart();
                    if (pending) {
                        Y_DEBUG_ABORT_UNLESS(pending->Get(index));
                        pending->Reset(index);
                    }
                } else {
                    if (AllocatingChunkPerSlotSize.insert(slotSize).second) {
                        auto aid = ctx.RegisterWithSameMailbox(new THullHugeBlobChunkAllocator(HugeKeeperCtx,
                            State.Pers, {}, slotSize));
                        ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                    }
                    done = false;
                    SlotSizeToTask.emplace(slotSize, task);
                    if (pending) {
                        Y_DEBUG_ABORT_UNLESS(pending->Get(index));
                        return false;
                    }
                    task->Pending[slotSize].Set(index);
                }
                return true;
            };

            if (slotSizeToProcess) {
                const auto it = task->Pending.find(slotSizeToProcess);
                Y_VERIFY_S(it != task->Pending.end(), HugeKeeperCtx->VCtx->VDiskLogPrefix);
                auto& pending = it->second;
                Y_FOR_EACH_BIT(index, pending) {
                    if (!processItem(index, &pending)) {
                        break;
                    }
                }
            } else {
                for (size_t i = 0; i < task->BlobSizes.size(); ++i) {
                    processItem(i, nullptr);
                }
            }

            if (done) {
                LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLHUGE, HugeKeeperCtx->VCtx->VDiskLogPrefix
                    << "TEvHugeAllocateSlotsResult# " << FormatList(task->Result));
                Send(task->Sender, new TEvHugeAllocateSlotsResult(std::move(task->Result)), 0, task->Cookie);
            }
        }

        void ProcessAllocateSlotTasks(ui32 slotSize, const TActorContext& ctx) {
            auto it = SlotSizeToTask.lower_bound(std::make_tuple(slotSize, nullptr));
            while (it != SlotSizeToTask.end() && std::get<0>(*it) == slotSize) {
                auto node = SlotSizeToTask.extract(it++);
                TryToFulfillTask(std::get<1>(node.value()), slotSize, ctx);
            }
        }

        void Handle(TEvHugeAllocateSlots::TPtr ev, const TActorContext& ctx) {
            LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLHUGE, HugeKeeperCtx->VCtx->VDiskLogPrefix
                << "TEvHugeAllocateSlots# " << FormatList(ev->Get()->BlobSizes));
            TryToFulfillTask(std::make_shared<TAllocateSlotsTask>(ev), 0, ctx);
        }

        void Handle(TEvHugeDropAllocatedSlots::TPtr ev, const TActorContext& /*ctx*/) {
            for (const auto& p : ev->Get()->Locations) {
                State.Pers->Heap->Free(p);
                const bool deleted = State.Pers->DeleteSlotInFlight(State.Pers->Heap->ConvertDiskPartToHugeSlot(p));
                Y_VERIFY_S(deleted, HugeKeeperCtx->VCtx->VDiskLogPrefix);
            }
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
                HFunc(TEvHugeAllocateSlots, Handle)
                HFunc(TEvHugeDropAllocatedSlots, Handle)
                HFunc(TEvHugePreCompact, Handle)
                HFunc(TEvHugeLockChunks, Handle)
                HFunc(TEvHugeStat, Handle)
                HFunc(TEvHugeShredNotify, Handle)
                HFunc(TEvListChunks, Handle)
                FFunc(TEvBlobStorage::EvHugeQueryForbiddenChunks, HandleQueryForbiddenChunks)
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
            Y_VERIFY_S(State.Pers->Recovered && State.Pers->SlotsInFlight.empty(),
                HugeKeeperCtx->VCtx->VDiskLogPrefix);
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
