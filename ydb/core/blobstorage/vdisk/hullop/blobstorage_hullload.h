#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

#include <util/generic/hash_set.h>

namespace NKikimr {

    template<typename TMemRec>
    struct TSmallBlobChunkIdxExtractor {
        template<typename TIterator>
        TMaybe<TChunkIdx> ExtractChunkIdx(TIterator& /*it*/) {
            return {};
        }
    };

    template<>
    struct TSmallBlobChunkIdxExtractor<TMemRecLogoBlob> {
        template<typename TIterator>
        TMaybe<TChunkIdx> ExtractChunkIdx(TIterator& it) {
            if (it->MemRec.GetType() == TBlobType::DiskBlob) {
                TDiskDataExtractor extr;
                it.GetDiskData(&extr);
                const TDiskPart& part = extr.SwearOne();
                if (!part.Empty()) {
                    return part.ChunkIdx;
                }
            }
            return {};
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullSegLoaded
    ////////////////////////////////////////////////////////////////////////////
    template <class TLevelSegment>
    struct THullSegLoaded : public TEventLocal<THullSegLoaded<TLevelSegment>, TEvBlobStorage::EvHullSegLoaded> {
        TIntrusivePtr<TLevelSegment> LevelSegment;

        THullSegLoaded(TIntrusivePtr<TLevelSegment> seg)
            : LevelSegment(std::move(seg))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAllChunksBuilder
    ////////////////////////////////////////////////////////////////////////////
    class TAllChunksBuilder {
    public:
        TAllChunksBuilder()
            : AllChunks()
            , LastChunkIdx(0)
        {}

        void Add(ui32 chunkIdx) {
            Y_DEBUG_ABORT_UNLESS(chunkIdx);
            if (chunkIdx != LastChunkIdx) {
                LastChunkIdx = chunkIdx;
                AllChunks.insert(chunkIdx);
            }
        }

        void FillInVector(TVector<ui32> &vec) {
            Y_DEBUG_ABORT_UNLESS(vec.empty());
            vec.reserve(AllChunks.size());
            for (const auto &x : AllChunks)
                vec.push_back(x);
        }

        TString ToString() const {
            TStringStream str;
            if (AllChunks.empty()) {
                return "<empty>";
            } else {
                str << "[";
                bool first = true;
                for (const auto &x : AllChunks) {
                    if (first)
                        first = false;
                    else
                        str << " ";
                    str << x;
                }
                str << "]";
                return str.Str();
            }
        }

    private:
        THashSet<ui32> AllChunks;
        ui32 LastChunkIdx;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TLevelSegmentLoader
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelSegmentLoader : public TActorBootstrapped< TLevelSegmentLoader<TKey, TMemRec> > {
        typedef ::NKikimr::TLevelSegmentLoader<TKey, TMemRec> TThis;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;

        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TLevelSegment> LevelSegment;
        TActorId Recipient;
        TString Origin;
        bool FirstRead;
        ui32 RestToReadIndex;
        ui32 RestToReadOutbound;
        TAllChunksBuilder Chunks;

        friend class TActorBootstrapped<TThis>;


        void Bootstrap(const TActorContext &ctx) {
            const TDiskPart &entry = LevelSegment->GetEntryPoint();
            ctx.Send(PDiskCtx->PDiskId, new NPDisk::TEvChunkRead(PDiskCtx->Dsk->Owner,
                                                        PDiskCtx->Dsk->OwnerRound,
                                                        entry.ChunkIdx, entry.Offset, entry.Size,
                                                        NPriRead::HullLoad, nullptr));
            Chunks.Add(entry.ChunkIdx);
            TThis::Become(&TThis::StateFunc);
        }

        // loaded index to string
        TString ToString() const {
            TStringStream str;
            {
                typedef typename TLevelSegment::TRec TRec;
                const char *b = LevelSegment->LoadedIndex.Data();
                const char *e = b + LevelSegment->LoadedIndex.Size();
                const TRec *begin = (const TRec *)b;
                const TRec *end = (const TRec *)e;
                str << "LOADER(" << (const void*)this << "): INDEX: ";
                for (const TRec *i = begin; i != end; i++) {
                    str << " " << i->ToString();
                }
            }
            {
                const char *b = LevelSegment->LoadedOutbound.Data();
                const char *e = b + LevelSegment->LoadedOutbound.Size();
                const TDiskPart *begin = (const TDiskPart *)b;
                const TDiskPart *end = (const TDiskPart *)e;
                str << " OUTBOUND:";
                if (begin == end)
                    str << " empty";
                else {
                    for (const TDiskPart *i = begin; i != end; i++) {
                        str << " " << i->ToString();
                    }
                }
            }
            return str.Str();
        }

        void Finish(const TActorContext &ctx) {
            Y_ABORT_UNLESS(RestToReadIndex == 0 && RestToReadOutbound == 0);

            // add data chunks to ChunksBuilder
            typedef typename TLevelSegment::TMemIterator TMemIterator;
            TSmallBlobChunkIdxExtractor<TMemRec> chunkIdxExtr;
            TMemIterator it(LevelSegment.Get());
            it.SeekToFirst();
            bool first = true;
            TKey prevKey;
            while (it.Valid()) {
                if (const TMaybe<TChunkIdx> chunkIdx = chunkIdxExtr.ExtractChunkIdx(it)) {
                    Chunks.Add(*chunkIdx);
                }

                // ensure that keys are stored in strictly increasing order
                const TKey& key = it.GetCurKey();
                if (first) {
                    first = false;
                } else {
                    Y_ABORT_UNLESS(prevKey < key && !prevKey.IsSameAs(key));
                }
                prevKey = key;

                it.Next();
            }
            Chunks.FillInVector(LevelSegment->AllChunks);

            ctx.Send(Recipient, new THullSegLoaded<TLevelSegment>(LevelSegment));
            TThis::Die(ctx);
        }

        void AppendIndexData(const char *data, size_t size) {
            Y_DEBUG_ABORT_UNLESS(data && size && RestToReadIndex >= size);

            RestToReadIndex -= size;
            memcpy(reinterpret_cast<char *>(LevelSegment->LoadedIndex.data()) + RestToReadIndex, data, size);
        }

        void AppendData(const char *data, size_t size) {
            Y_DEBUG_ABORT_UNLESS(data && size);

            if (RestToReadOutbound) {
                if (RestToReadOutbound >= size) {
                    RestToReadOutbound -= size;
                    memcpy(reinterpret_cast<char *>(LevelSegment->LoadedOutbound.data()) + RestToReadOutbound, data, size);
                    return;
                } else {
                    size_t writeSize = RestToReadOutbound;
                    size_t restSize = size - writeSize;

                    memcpy(reinterpret_cast<char *>(LevelSegment->LoadedOutbound.data()), data + restSize, writeSize);
                    RestToReadOutbound = 0;

                    AppendIndexData(data, restSize);
                }
            } else {
                AppendIndexData(data, size);
            }
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr &ev, const TActorContext &ctx) {
            auto *msg = ev->Get();
            CHECK_PDISK_RESPONSE_READABLE_MSG(VCtx, ev, ctx, TStringBuilder() << "{Origin# '" << Origin << "'}");

            const TBufferWithGaps &data = msg->Data;
            LevelSegment->IndexParts.push_back({msg->ChunkIdx, msg->Offset, msg->Data.Size()});

            if (FirstRead) {
                FirstRead = false;

                // copy placeholder data, because otherwise we get unaligned access
                TIdxDiskPlaceHolder placeHolder(0);
                size_t partSize = data.Size() - sizeof(TIdxDiskPlaceHolder);
                memcpy(&placeHolder, data.DataPtr<const TIdxDiskPlaceHolder>(partSize), sizeof(TIdxDiskPlaceHolder));

                Y_ABORT_UNLESS(placeHolder.MagicNumber == TIdxDiskPlaceHolder::Signature);
                RestToReadIndex = placeHolder.Info.IdxTotalSize;
                RestToReadOutbound = placeHolder.Info.OutboundItems * sizeof(TDiskPart);
                LevelSegment->LoadedIndex.resize(placeHolder.Info.Items);
                LevelSegment->LoadedOutbound.resize(placeHolder.Info.OutboundItems);
                LevelSegment->Info = placeHolder.Info;
                LevelSegment->AssignedSstId = placeHolder.SstId;

                AppendData(data.DataPtr<const char>(0, partSize), partSize);

                if (!placeHolder.PrevPart.Empty()) {
                    ctx.Send(PDiskCtx->PDiskId, new NPDisk::TEvChunkRead(PDiskCtx->Dsk->Owner,
                                                                 PDiskCtx->Dsk->OwnerRound,
                                                                 placeHolder.PrevPart.ChunkIdx,
                                                                 placeHolder.PrevPart.Offset,
                                                                 placeHolder.PrevPart.Size, NPriRead::HullLoad,
                                                                 nullptr));
                    Chunks.Add(placeHolder.PrevPart.ChunkIdx);
                } else {
                    Finish(ctx);
                }
            } else {
                TIdxDiskLinker linker;
                size_t partSize = data.Size() - sizeof(TIdxDiskLinker);
                memcpy(&linker, data.DataPtr<const TIdxDiskLinker>(partSize), sizeof(TIdxDiskLinker));

                // TODO(alexvru, fomichev): this logic seems to work incorrectly -- data must be prepended
                AppendData(data.DataPtr<const char>(0, partSize), partSize);

                if (!linker.PrevPart.Empty()) {
                    ctx.Send(PDiskCtx->PDiskId, new NPDisk::TEvChunkRead(PDiskCtx->Dsk->Owner,
                                                                 PDiskCtx->Dsk->OwnerRound,
                                                                 linker.PrevPart.ChunkIdx,
                                                                 linker.PrevPart.Offset, linker.PrevPart.Size,
                                                                 NPriRead::HullLoad, nullptr));
                    Chunks.Add(linker.PrevPart.ChunkIdx);
                } else {
                    Finish(ctx);
                }
            }
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(NPDisk::TEvChunkReadResult, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_SEGMENT_LOADER;
        }

        TLevelSegmentLoader(
                const TVDiskContextPtr &vctx,
                const TPDiskCtxPtr &pdiskCtx,
                TIntrusivePtr<TLevelSegment> levelSegment,
                const TActorId &recipient,
                const TString &origin)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , LevelSegment(std::move(levelSegment))
            , Recipient(recipient)
            , Origin(origin)
            , FirstRead(true)
            , RestToReadIndex(0)
            , RestToReadOutbound(0)
            , Chunks()
        {
            const TDiskPart& entry = LevelSegment->GetEntryPoint();
            Y_DEBUG_ABORT_UNLESS(!entry.Empty());
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // THullSegmentsLoaded
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct THullSegmentsLoaded : public TEventLocal<THullSegmentsLoaded<TKey, TMemRec>, TEvBlobStorage::EvHullSegmentsLoaded> {
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TUnorderedLevelSegments;
        TIntrusivePtr<TOrderedLevelSegments> OrderedSegs;
        TIntrusivePtr<TUnorderedLevelSegments> UnorderedSegs;

        THullSegmentsLoaded(TIntrusivePtr<TOrderedLevelSegments> segs)
            : OrderedSegs(std::move(segs))
        {}

        THullSegmentsLoaded(TIntrusivePtr<TUnorderedLevelSegments> segs)
            : UnorderedSegs(std::move(segs))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TOrderedLevelSegmentsLoader
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TOrderedLevelSegmentsLoader : public TActorBootstrapped< TOrderedLevelSegmentsLoader<TKey, TMemRec> > {
        typedef ::NKikimr::TOrderedLevelSegmentsLoader<TKey, TMemRec> TThis;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::THullSegLoaded<TLevelSegment> THullSegLoaded;
        typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
        typedef ::NKikimr::TLevelSegmentLoader<TKey, TMemRec> TLevelSegmentLoader;
        typedef ::NKikimr::THullSegmentsLoaded<TKey, TMemRec> THullSegmentsLoaded;


        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TOrderedLevelSegments> Segs;
        TActorId Recipient;
        ui32 Pos;
        ui32 Size;
        TActiveActors ActiveActors;

        friend class TActorBootstrapped<TThis>;

        void Process(const TActorContext &ctx) {
            if (Pos < Size) {
                std::unique_ptr<TLevelSegmentLoader> actor(new TLevelSegmentLoader(VCtx, PDiskCtx,
                        Segs->Segments[Pos].Get(), ctx.SelfID, "OrderedLevelSegmentsLoader"));
                NActors::TActorId aid = ctx.Register(actor.Release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                ++Pos;
            } else {
                Y_DEBUG_ABORT_UNLESS(Pos == Size);
                ctx.Send(Recipient, new THullSegmentsLoaded(std::move(Segs)));
                TThis::Die(ctx);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            Process(ctx);
        }

        void Handle(typename THullSegLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Process(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HTemplFunc(THullSegLoaded, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_ORD_LEVEL_SEGMENT_LOADER;
        }

        TOrderedLevelSegmentsLoader(
                const TVDiskContextPtr vctx,
                const TPDiskCtxPtr pdiskCtx,
                TIntrusivePtr<TOrderedLevelSegments> levelSegmentVec,
                const TActorId &recipient)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , Segs(std::move(levelSegmentVec))
            , Recipient(recipient)
            , Pos(0)
            , Size(Segs->Segments.size())
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TUnorderedLevelSegmentsLoader
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TUnorderedLevelSegmentsLoader : public TActorBootstrapped< TUnorderedLevelSegmentsLoader<TKey, TMemRec> > {
        typedef ::NKikimr::TUnorderedLevelSegmentsLoader<TKey, TMemRec> TThis;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::THullSegLoaded<TLevelSegment> THullSegLoaded;
        typedef ::NKikimr::TUnorderedLevelSegments<TKey, TMemRec> TUnorderedLevelSegments;
        typedef ::NKikimr::TLevelSegmentLoader<TKey, TMemRec> TLevelSegmentLoader;
        typedef ::NKikimr::THullSegmentsLoaded<TKey, TMemRec> THullSegmentsLoaded;

        typedef typename TUnorderedLevelSegments::TSegments TSegments;
        typedef typename TSegments::iterator TIterator;

        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TUnorderedLevelSegments> Segs;
        TActorId Recipient;
        TIterator Pos;
        TIterator End;
        TActiveActors ActiveActors;

        friend class TActorBootstrapped<TThis>;

        void Process(const TActorContext &ctx) {
            if (Pos != End) {
                std::unique_ptr<TLevelSegmentLoader> actor(new TLevelSegmentLoader(VCtx, PDiskCtx, Pos->Get(), ctx.SelfID));
                NActors::TActorId aid = ctx.Register(actor.Release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            } else {
                ctx.Send(Recipient, new THullSegmentsLoaded(std::move(Segs)));
                TThis::Die(ctx);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            Process(ctx);
        }

        void Handle(typename THullSegLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            ++Pos;
            Process(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HTemplFunc(THullSegLoaded, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_UNORD_LEVEL_SEGMENT_LOADER;
        }

        TUnorderedLevelSegmentsLoader(
                const TVDiskContextPtr &vctx,
                const TPDiskCtxPtr &pdiskCtx,
                TIntrusivePtr<TUnorderedLevelSegments> segs, const TActorId &recipient)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , Segs(std::move(segs))
            , Recipient(recipient)
            , Pos(Segs->Segments.begin())
            , End(Segs->Segments.end())
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // THullIndexLoaded
    ////////////////////////////////////////////////////////////////////////////
    struct THullIndexLoaded : public TEventLocal<THullIndexLoaded, TEvBlobStorage::EvHullIndexLoaded> {
        EHullDbType Type;

        THullIndexLoaded(EHullDbType type)
            : Type(type)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexLoader
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, EHullDbType type>
    class TLevelIndexLoader : public TActorBootstrapped<TLevelIndexLoader<TKey, TMemRec, type>> {
        typedef ::NKikimr::TLevelIndexLoader<TKey, TMemRec, type> TThis;
        typedef ::NKikimr::TLevelIndex<TKey, TMemRec> TLevelIndex;
        typedef ::NKikimr::TLevelSegmentLoader<TKey, TMemRec> TLevelSegmentLoader;
        typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
        typedef ::NKikimr::THullSegLoaded<TLevelSegment> THullSegLoaded;
        typedef ::NKikimr::TLevelSlice<TKey, TMemRec> TLevelSlice;
        typedef typename TLevelSlice::TSstIterator TSstIterator;

        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TLevelIndex> LevelIndex;
        TActorId Recipient;
        TSstIterator It;
        TActiveActors ActiveActors;

        friend class TActorBootstrapped<TThis>;

        void Process(const TActorContext &ctx) {
            if (It.Valid()) {
                // Load next
                auto actor = std::make_unique<TLevelSegmentLoader>(VCtx, PDiskCtx, It.Get().SstPtr.Get(), ctx.SelfID,
                    "LevelIndexLoader");
                NActors::TActorId aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                It.Next();
            } else {
                // Done
                LevelIndex->LoadCompleted();
                ctx.Send(Recipient, new THullIndexLoaded(type));
                TThis::Die(ctx);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            It.SeekToFirst();
            Process(ctx);
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(typename THullSegLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Process(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HTemplFunc(THullSegLoaded, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_LOADER;
        }

        TLevelIndexLoader(
                const TVDiskContextPtr vctx,
                const TPDiskCtxPtr pdiskCtx,
                TIntrusivePtr<TLevelIndex> levelIndex,
                const TActorId &recipient)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , LevelIndex(std::move(levelIndex))
            , Recipient(recipient)
            , It(LevelIndex->CurSlice.Get(), LevelIndex->CurSlice->Level0CurSstsNum())
        {}
    };

} // NKikimr
