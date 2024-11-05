#include "dataset.h"

#include <util/generic/set.h>
#include <util/random/shuffle.h>

using namespace NKikimr;

extern const ui64 DefaultTestTabletId = 5000;

TString CreateData(const TString &orig, ui32 minHugeBlobSize, bool huge) {
    if (huge) {
        TString res;
        res.reserve(minHugeBlobSize + orig.size());
        while (res.size() < minHugeBlobSize) {
            res += orig;
        }
        return res;
    } else
        return orig;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSmallCommonDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TSmallCommonDataSet::TSmallCommonDataSet() {
    auto put = [&] (ui32 step, TString data) {
        Items.push_back(TDataItem{NKikimr::TLogoBlobID(DefaultTestTabletId, 1, step, 0, data.size(), 0), std::move(data),
            NKikimrBlobStorage::EPutHandleClass::TabletLog});
    };

    put(322, "xxxxxxxxxx");
    put(370, "yyy");
    put(424, "zzz");
    put(472, "pppp");
    put(915, "qqqqq");
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCustomDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TCustomDataSet::TCustomDataSet(ui64 tabletId, ui32 gen, ui32 channel, ui32 step, ui32 num, ui32 blobSize,
                               NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge) {
    Y_UNUSED(blobSize); // FIXME: incorrect data size for small blobs
    TString data(CreateData("abcdefghkj", minHugeBlobSize, huge));
    for (ui32 i = 0; i < num; i++) {
        NKikimr::TLogoBlobID id(tabletId, gen, step + i, channel, data.size(), 0);
        Items.push_back(TDataItem{id, data, cls});
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T3PutDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
T3PutDataSet::T3PutDataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge) {
    TString abcdefghkj(CreateData("abcdefghkj", minHugeBlobSize, huge));
    TLogoBlobID id1(DefaultTestTabletId, 1, 16, 0, abcdefghkj.size(), 0, 1);
    Items.push_back(TDataItem(id1, abcdefghkj, cls));

    TString pqr(CreateData("pqr", minHugeBlobSize, huge));
    TLogoBlobID id2(DefaultTestTabletId, 1, 30, 0, pqr.size(), 0, 1);
    Items.push_back(TDataItem(id2, pqr, cls));

    TString xyz(CreateData("xyz", minHugeBlobSize, huge));
    TLogoBlobID id3(DefaultTestTabletId, 1, 36, 0, xyz.size(), 0, 1);
    Items.push_back(TDataItem(id3, xyz, cls));
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T1PutHandoff2DataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
T1PutHandoff2DataSet::T1PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge) {
    TString abc(CreateData("abc", minHugeBlobSize, huge));
    TLogoBlobID id1Part1(DefaultTestTabletId, 1, 16, 0, abc.size(), 0, 1);
    TLogoBlobID id1Part2(DefaultTestTabletId, 1, 16, 0, abc.size(), 0, 2);

    Items.push_back(TDataItem(id1Part1, abc, cls));
    Items.push_back(TDataItem(id1Part2, abc, cls));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T3PutHandoff2DataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
T3PutHandoff2DataSet::T3PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge) {
    TString abcdefghkj(CreateData("abcdefghkj", minHugeBlobSize, huge));
    TString pqr(CreateData("pqr", minHugeBlobSize, huge));
    TString xyz(CreateData("xyz", minHugeBlobSize, huge));
    TLogoBlobID id1Part1(DefaultTestTabletId, 1, 16, 0, abcdefghkj.size(), 0, 1);
    TLogoBlobID id1Part2(DefaultTestTabletId, 1, 16, 0, abcdefghkj.size(), 0, 2);
    TLogoBlobID id2Part1(DefaultTestTabletId, 1, 30, 0, pqr.size(), 0, 1);
    TLogoBlobID id2Part2(DefaultTestTabletId, 1, 30, 0, pqr.size(), 0, 2);
    TLogoBlobID id3Part1(DefaultTestTabletId, 1, 36, 0, xyz.size(), 0, 1);
    TLogoBlobID id3Part2(DefaultTestTabletId, 1, 36, 0, xyz.size(), 0, 2);

    Items.push_back(TDataItem(id1Part1, abcdefghkj, cls));
    Items.push_back(TDataItem(id1Part2, abcdefghkj, cls));
    Items.push_back(TDataItem(id2Part1, pqr, cls));
    Items.push_back(TDataItem(id2Part2, pqr, cls));
    Items.push_back(TDataItem(id3Part1, xyz, cls));
    Items.push_back(TDataItem(id3Part2, xyz, cls));
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateBlobGenerator
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
IDataGenerator* CreateBlobGenerator(ui64 maxCumSize, ui32 maxNumBlobs, ui32 minBlobSize, ui32 maxBlobSize,
        ui32 differentTablets, ui32 startingStep, TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> info,
        TVector<TVDiskID> matchingVDisks, bool reuseData)
{
    class Generator : public IDataGenerator
    {
    public:
        Generator(ui64 maxCumSize, ui32 maxNumBlobs, ui32 minBlobSize, ui32 maxBlobSize, ui32 differentTablets,
            ui32 startingStep, TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> info, TVector<TVDiskID> matchingVDisks,
            bool reuseData)
            : Rng(1, 1)
            , MaxCumSize(maxCumSize)
            , MaxNumBlobs(maxNumBlobs)
            , MinBlobSize(minBlobSize)
            , MaxBlobSize(maxBlobSize)
            , DifferentTablets(differentTablets)
            , Step(startingStep)
            , CumSize(0)
            , NumBlobs(0)
            , Info(info)
            , MatchingVDisks(std::move(matchingVDisks))
            , ReuseData(reuseData)
        {}

        bool Next(TDataItem& item) override {
            if (CumSize >= MaxCumSize || NumBlobs >= MaxNumBlobs)
                return false;

            const ui32 size = MinBlobSize + Rng() % (MaxBlobSize - MinBlobSize + 1);

            // generate pseudorandom data with step and size seed
            TString data = GenerateData(size);

            ui32 numDisks = Info->Type.BlobSubgroupSize();
            ui32 handoff = Info->Type.Handoff();

            // find matching LogoBlobID
            TLogoBlobID id;
            const ui64 tabletId = GenerateTabletId();
            bool goodId = false;
            do {
                id = TLogoBlobID(tabletId, 1, ++Step, 0, size, 0);
                ui32 hash = id.Hash();
                for (const TVDiskID& vDiskID : MatchingVDisks) {
                    TBlobStorageGroupInfo::TVDiskIds vDisks;
                    TBlobStorageGroupInfo::TServiceIds services;
                    Info->PickSubgroup(hash, &vDisks, &services);
                    for (ui32 i = 0; i < numDisks - handoff; ++i) {
                        if (vDisks[i] == vDiskID) {
                            goodId = true;
                            break;
                        }
                    }
                    if (goodId)
                        break;
                }
            } while (!goodId);

            // store item
            item = TDataItem(id, std::move(data), NKikimrBlobStorage::EPutHandleClass::AsyncBlob);

            // adjust counters
            CumSize += size;
            ++NumBlobs;
            return true;
        }

        IDataGenerator* Clone() override {
            return new Generator(*this);
        }

        TString GenerateData(ui32 size) {
            if (ReuseData) {
                if (size > ReusedDataBuf.size())
                    ReusedDataBuf.resize(size);
                return ReusedDataBuf;
            } else {
                // generate pseudorandom data with step and size seed
                TString data;
                data.reserve(size);
                for (ui32 i = 0; i < size; ++i)
                    data.push_back(0x20 + Rng() % 0x7f);
                return data;
            }
        }

        ui64 GenerateTabletId() {
            if (DifferentTablets > 1) {
                return Rng() % DifferentTablets + DefaultTestTabletId;
            } else {
                return DefaultTestTabletId;
            }
        }

    private:
        TFastRng32 Rng;
        const ui64 MaxCumSize;
        const ui32 MaxNumBlobs;
        const ui32 MinBlobSize;
        const ui32 MaxBlobSize;
        const ui32 DifferentTablets;
        ui32 Step;
        ui64 CumSize;
        ui32 NumBlobs;
        TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> Info;
        TVector<TVDiskID> MatchingVDisks;
        bool ReuseData;
        TString ReusedDataBuf;
    };

    return new Generator(maxCumSize, maxNumBlobs, minBlobSize, maxBlobSize, differentTablets, startingStep, info,
            std::move(matchingVDisks), reuseData);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBadIdsDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TBadIdsDataSet::TBadIdsDataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge) {
    TString abcdefghkj(CreateData("abcdefghkj", minHugeBlobSize, huge));
    ui64 tabletId = 0;
    TLogoBlobID id(tabletId, 1, 10, 0, abcdefghkj.size(), 0, 1);
    Items.push_back(TDataItem(id, abcdefghkj, cls));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
