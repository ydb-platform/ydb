#include "blobstorage_syncer_localwriter.h"
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

namespace NKikimr {

    TEvLocalSyncData::TEvLocalSyncData(const TVDiskID &vdisk, const TSyncState &syncState, const TString &data)
        : VDiskID(vdisk)
        , SyncState(syncState)
        , Data(data)
    {}

    TString TEvLocalSyncData::Serialize() const {
        TStringStream str;
        Serialize(str);
        return str.Str();
    }

    void TEvLocalSyncData::Serialize(IOutputStream &s) const {
        VDiskID.Serialize(s);
        SyncState.Serialize(s);
        ui32 size = Data.size();
        s.Write(&size, sizeof(size));
        s.Write(Data.data(), size);
    }

    bool TEvLocalSyncData::Deserialize(IInputStream &s) {
        if (!VDiskID.Deserialize(s))
            return false;
        if (!SyncState.Deserialize(s))
            return false;
        ui32 size = 0;
        if (s.Load(&size, sizeof(size)) != sizeof(size))
            return false;
        TVector<char> array;
        array.resize(size);
        if (s.Load(&array[0], size) != size)
            return false;
        Data.assign(&array[0], size);
        return true;
    }

    template <class TKey, class TMemRec>
    void SqueezeAppendixVec(typename TFreshAppendix<TKey, TMemRec>::TVec &vec) {
        if (!vec)
            return;

        std::sort(vec.begin(), vec.end());
        TKey key = vec[0].Key;
        size_t uniqueKeys = 1;
        // count uniq tablets
        for (size_t i = 0; i < vec.size(); ++i) {
            const auto &rec = vec[i];
            if (!(key == rec.Key)) {
                key = rec.Key;
                ++uniqueKeys;
            }
        }
        // reserve
        typename TFreshAppendix<TKey, TMemRec>::TVec squeezed;
        squeezed.reserve(uniqueKeys);
        // squeeze
        key = vec[0].Key;
        TMemRec memRec = vec[0].MemRec;
        for (size_t i = 0; i < vec.size(); ++i) {
            const auto &rec = vec[i];
            if (!(key == rec.Key)) {
                squeezed.emplace_back(key, memRec);
                key = rec.Key;
                memRec = rec.MemRec;
            } else {
                memRec.Merge(rec.MemRec, key);
            }
        }
        squeezed.emplace_back(key, memRec);
        vec.swap(squeezed);
    }

    void TEvLocalSyncData::Squeeze(TFreshAppendixLogoBlobs::TVec &vec) {
        SqueezeAppendixVec<TKeyLogoBlob, TMemRecLogoBlob>(vec);
    }

    void TEvLocalSyncData::Squeeze(TFreshAppendixBlocks::TVec &vec) {
        SqueezeAppendixVec<TKeyBlock, TMemRecBlock>(vec);
    }

    void TEvLocalSyncData::Squeeze(TFreshAppendixBarriers::TVec &vec) {
        SqueezeAppendixVec<TKeyBarrier, TMemRecBarrier>(vec);
    }

    void TEvLocalSyncData::UnpackData(const TIntrusivePtr<TVDiskContext> &vctx) {
        Extracted.Clear();

        TFreshAppendixLogoBlobs::TVec logoBlobs;
        TFreshAppendixBlocks::TVec blocks;
        TFreshAppendixBarriers::TVec barriers;

        // record handlers
        auto blobHandler = [&] (const NSyncLog::TLogoBlobRec *rec) {
            Y_VERIFY_DEBUG(TIngress::MustKnowAboutLogoBlob(vctx->Top.get(), vctx->ShortSelfVDisk, rec->LogoBlobID()),
                    "logoBlobID# %s ShortSelfVDisk# %s top# %s", rec->LogoBlobID().ToString().data(),
                    vctx->ShortSelfVDisk.ToString().data(), vctx->Top->ToString().data());

            TLogoBlobID id(rec->LogoBlobID(), 0); // TODO: add verify for logoBlob.PartId() == 0 after migration
            logoBlobs.emplace_back(TKeyLogoBlob(id), TMemRecLogoBlob(rec->Ingress));
        };
        auto blockHandler = [&] (const NSyncLog::TBlockRec *rec) {
            blocks.emplace_back(TKeyBlock(rec->TabletId), TMemRecBlock(rec->Generation));
        };
        auto barrierHandler = [&] (const NSyncLog::TBarrierRec *rec) {
            TKeyBarrier keyBarrier(rec->TabletId, rec->Channel, rec->Gen, rec->GenCounter, rec->Hard);
            TMemRecBarrier memRecBarrier(rec->CollectGeneration, rec->CollectStep, rec->Ingress);
            barriers.emplace_back(keyBarrier, memRecBarrier);
        };
        auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2 *rec) {
            blocks.emplace_back(TKeyBlock(rec->TabletId), TMemRecBlock(rec->Generation));
        };

        // process synclog data
        NSyncLog::TFragmentReader fragment(Data);
        fragment.ForEach(blobHandler, blockHandler, barrierHandler, blockHandlerV2);

        if (logoBlobs) {
            Squeeze(logoBlobs);
            Extracted.LogoBlobs =
                std::make_shared<TFreshAppendixLogoBlobs>(std::move(logoBlobs), vctx->FreshIndex, true);
        }
        if (blocks) {
            Squeeze(blocks);
            // blocks are already sorted
            Extracted.Blocks =
                std::make_shared<TFreshAppendixBlocks>(std::move(blocks), vctx->FreshIndex, true);
        }
        if (barriers) {
            Squeeze(barriers);
            Extracted.Barriers =
                std::make_shared<TFreshAppendixBarriers>(std::move(barriers), vctx->FreshIndex, true);
        }
        Y_VERIFY(Extracted.IsReady());
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // TLocalSyncDataExtractorActor -- actor extracts data from TEvLocalSyncData
    ///////////////////////////////////////////////////////////////////////////////////////////////
    class TLocalSyncDataExtractorActor : public TActorBootstrapped<TLocalSyncDataExtractorActor> {
    protected:
        friend class TActorBootstrapped<TLocalSyncDataExtractorActor>;

        TIntrusivePtr<TVDiskContext> VCtx;
        TActorId SkeletonId;
        TActorId ParentId;
        std::unique_ptr<TEvLocalSyncData> Ev;

        void Bootstrap(const TActorContext &ctx) {
            auto startTime = TAppData::TimeProvider->Now();
            Ev->UnpackData(VCtx);
            auto finishTime = TAppData::TimeProvider->Now();
            LOG_DEBUG_S(ctx, NKikimrServices::BS_SYNCER, VCtx->VDiskLogPrefix
                    << "TLocalSyncDataExtractorActor: VDiskId# " << Ev->VDiskID.ToString()
                    << " dataSize# " << Ev->Data.size()
                    << " duration# %s" << (finishTime - startTime));

            ctx.Send(new IEventHandle(SkeletonId, ParentId, Ev.release()));
            PassAway();
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::VDISK_LOCALSYNCDATA_EXTRACTOR;
        }

        TLocalSyncDataExtractorActor(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TActorId &skeletonId,
                const TActorId &parentId,
                std::unique_ptr<TEvLocalSyncData> ev)
            : VCtx(vctx)
            , SkeletonId(skeletonId)
            , ParentId(parentId)
            , Ev(std::move(ev))
        {}
    };

    IActor *CreateLocalSyncDataExtractor(const TIntrusivePtr<TVDiskContext> &vctx, const TActorId &skeletonId,
        const TActorId &parentId, std::unique_ptr<TEvLocalSyncData> ev) {
        return new TLocalSyncDataExtractorActor(vctx, skeletonId, parentId, std::move(ev));
    }


} // NKikimr
