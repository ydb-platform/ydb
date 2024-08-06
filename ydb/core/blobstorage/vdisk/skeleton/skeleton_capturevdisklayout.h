#pragma once

namespace NKikimr {

    class TCaptureVDiskLayoutActor : public TActorBootstrapped<TCaptureVDiskLayoutActor> {
        using TRes = TEvBlobStorage::TEvCaptureVDiskLayoutResult;

        TEvBlobStorage::TEvCaptureVDiskLayout::TPtr Ev;
        THullDsSnap Snap;

    public:
        TCaptureVDiskLayoutActor(TEvBlobStorage::TEvCaptureVDiskLayout::TPtr ev, THullDsSnap snap)
            : Ev(ev)
            , Snap(std::move(snap))
        {}

        void Bootstrap() {
            auto res = std::make_unique<TRes>();

            auto traverse = [&](const auto& slice, auto&& callback, TRes::EDatabase type) {
                using T = std::decay_t<decltype(slice)>;
                typename T::TSstIterator iter(&slice);
                for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                    const auto& item = iter.Get();
                    const auto& sst = item.SstPtr;
                    for (const TDiskPart& part : sst->IndexParts) {
                        res->Layout.push_back({part, type, TRes::ERecordType::IndexRecord, {}, sst->AssignedSstId,
                            item.Level});
                    }
                    callback(sst, item.Level);
                }
            };

            // then, scan all the blobs
            struct TMerger {
                TRes& Res;
                ui32 Level;

                static constexpr bool HaveToMergeData() { return true; }

                void Finish() {}

                void Clear() {}

                void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& key,
                        ui64 circaLsn) {
                    TDiskDataExtractor extr;
                    memRec.GetDiskData(&extr, outbound);
                    const TRes::ERecordType recordType = extr.BlobType == TBlobType::DiskBlob
                        ? TRes::ERecordType::InplaceBlob
                        : TRes::ERecordType::HugeBlob;
                    for (const TDiskPart *location = extr.Begin; location != extr.End; ++location) {
                        if (location->ChunkIdx && location->Size) {
                            Res.Layout.push_back({*location, TRes::EDatabase::LogoBlobs, recordType, key.LogoBlobID(),
                                circaLsn, Level});
                        }
                    }
                }

                void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope *data, const TKeyLogoBlob& key,
                        ui64 circaLsn) {
                    if (!data) {
                        AddFromSegment(memRec, nullptr, key, circaLsn);
                    }
                }
            };

            auto logoBlobsCallback = [&](const auto& sst, ui32 level) {
                TMerger merger{*res, level};
                TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TMemIterator iter(sst.Get());
                for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                    iter.PutToMerger(&merger);
                }
            };

            auto ignoreCallback = [](const auto& /*sst*/, ui32 /*level*/) {};

            traverse(Snap.LogoBlobsSnap.SliceSnap, logoBlobsCallback, TRes::EDatabase::LogoBlobs);
            traverse(Snap.BlocksSnap.SliceSnap, ignoreCallback, TRes::EDatabase::Blocks);
            traverse(Snap.BarriersSnap.SliceSnap, ignoreCallback, TRes::EDatabase::Barriers);

            TMerger merger{*res, Max<ui32>()};
            TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>::TForwardIterator iter(Snap.HullCtx, &Snap.LogoBlobsSnap.FreshSnap);
            THeapIterator<TKeyLogoBlob, TMemRecLogoBlob, true> heapIt(&iter);
            heapIt.Walk(TKeyLogoBlob::First(), &merger, [] (TKeyLogoBlob /*key*/, auto* /*merger*/) { return true; });

            Send(Ev->Sender, res.release(), 0, Ev->Cookie);
            PassAway();
        }
    };

} // NKikimr
