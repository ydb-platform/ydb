#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"
#include "hullds_settings.h"

namespace NKikimr {

    class TTestContexts {
    public:
        TTestContexts(ui32 chunkSize = 135249920, ui64 compWorthReadSize = (2ul << 20ul))
            : ChunkSize(chunkSize)
            , CompWorthReadSize(compWorthReadSize)
            , GroupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4)
            , VCtx(new TVDiskContext(TActorId(), GroupInfo.PickTopology(), new ::NMonitoring::TDynamicCounters(),
                        TVDiskID(), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN))
            , HullCtx(
                    new THullCtx(
                        VCtx,
                        ChunkSize,
                        CompWorthReadSize,
                        true,
                        true,
                        true,
                        true,
                        1,      // HullSstSizeInChunksFresh
                        1,      // HullSstSizeInChunksLevel
                        2.0,
                        10,     // FreshCompMaxInFlightWrites
                        10,     // HullCompMaxInFlightWrites
                        20,     // HullCompMaxInFlightReads
                        0.5,
                        TDuration::Minutes(5),
                        TDuration::Seconds(1),
                        true))  // AddHeader
            , LevelIndexSettings(
                HullCtx,
                8u,                         // Level0MaxSstsAtOnce
                64u << 20u,                 // BufSize
                0,                          // CompThreshold
                TDuration::Minutes(10),     // HistoryWindow
                10u,                        // HistoryBuckets
                false,
                false)
        {}

        inline TVDiskContextPtr GetVCtx() {
            return VCtx;
        }

        inline THullCtxPtr GetHullCtx() {
            return HullCtx;
        }

        const TLevelIndexSettings &GetLevelIndexSettings() const {
            return LevelIndexSettings;
        }

    private:
        const ui32 ChunkSize;
        const ui64 CompWorthReadSize;
        TBlobStorageGroupInfo GroupInfo;
        TVDiskContextPtr VCtx;
        THullCtxPtr HullCtx;
        TLevelIndexSettings LevelIndexSettings;
    };

    class TIntContainer : public TVector<int> {
    public:
        TIntContainer(const TVector<int> &src)
            : TVector<int>(src)
        {}

        bool Empty() const {
            return empty();
        }
    };

    struct TVectorIt {
        using TContType = TIntContainer;

        TIntContainer *Vec;
        TIntContainer::iterator It;

        TVectorIt(const THullCtxPtr &hullCtx, TContType *vec)
            : Vec(vec)
            , It()
        {
            Y_UNUSED(hullCtx);
        }

        TVectorIt() = default;

        bool Valid() const {
            return Vec && It >= Vec->begin() && It < Vec->end();
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++It;
        }

        void SetNoBlob() {
        }

        template<typename TKey>
        void Merge(const TVectorIt, TKey) {
        }

        TMemPart GetMemData() const{
            return TMemPart(0, 0);
        }

        int GetType() const{
            return 0;
        }

        bool HasData() const{
            return Valid();
        }

        void Prev() {
            if (It == Vec->begin())
                It = {};
            else
                --It;
        }

        int GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return *It;
        }

        void SeekToFirst() {
            It = Vec->begin();
        }

        void Seek(int key) {
            It = ::LowerBound(Vec->begin(), Vec->end(), key);
        }

        TString ToString() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Sprintf("%d", *It);
        }

        template <class TRecordMerger>
        void PutToMerger(TRecordMerger *merger) {
            merger->Add(*It);
        }

        template <class THeap>
        void PutToHeap(THeap& heap) {
            heap.Add(this);
        }
    };

} // NKikimr

