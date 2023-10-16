#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/defs.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////
    // TAddBulkSstEssence
    ////////////////////////////////////////////////////////////////////////////////
    struct TAddBulkSstEssence {

        // TSstAndRecsNum -- Sst + RecordsNumber
        template<class TLevelSegment>
        struct TSstAndRecsNum {
            // SSTable being added
            TIntrusivePtr<TLevelSegment> Sst;
            // number of records in this SSTable
            ui32 RecsNum = 0;

            TSstAndRecsNum(TIntrusivePtr<TLevelSegment> &&sst, ui32 recsNum)
                : Sst(std::move(sst))
                , RecsNum(recsNum)
            {}

            void Output(IOutputStream &str) const {
                str << "{RecsNum# " << RecsNum << "}";
            }
        };

        using TLogoBlobsAddition = TSstAndRecsNum<TLogoBlobsSst>;
        using TBlocksAddition = TSstAndRecsNum<TBlocksSst>;
        using TBarriersAddition = TSstAndRecsNum<TBarriersSst>;

        // additions to LogoBlobs DB
        TVector<TLogoBlobsAddition> LogoBlobsAdditions;
        // additions to Blocks DB
        TVector<TBlocksAddition> BlocksAdditions;
        // additions to Barriers DB
        TVector<TBarriersAddition> BarriersAdditions;

        TAddBulkSstEssence() = default;
        TAddBulkSstEssence(TAddBulkSstEssence &&) = default;
        TAddBulkSstEssence &operator =(TAddBulkSstEssence &&) = default;
        TAddBulkSstEssence(const TAddBulkSstEssence &) = delete;
        TAddBulkSstEssence &operator =(const TAddBulkSstEssence &) = delete;
        TAddBulkSstEssence(TLogoBlobsSstPtr &&oneLogoBlobsSst, ui32 oneLogoBlobsSstRecsNum) {
            LogoBlobsAdditions.emplace_back(std::move(oneLogoBlobsSst), oneLogoBlobsSstRecsNum);
        }

        void DestructiveMerge(TAddBulkSstEssence &&e);
        void SerializeToProto(NKikimrVDiskData::TAddBulkSstRecoveryLogRec &pb) const;
        void Output(IOutputStream &str) const;
        TString ToString() const;

        // FIXME: temporary method, get rid of explicit sending this message to THullActor
        // and remove this method
        template <class TKey, class TMemRec>
        TSstAndRecsNum<TLevelSegment<TKey, TMemRec>> EnsureOnlyOneSst() const;

        void Replace(TVector<TLogoBlobsAddition> &&vec) {
            LogoBlobsAdditions = std::move(vec);
        }
        void Replace(TVector<TBlocksAddition> &&vec) {
            BlocksAdditions = std::move(vec);
        }
        void Replace(TVector<TBarriersAddition> &&vec) {
            BarriersAdditions = std::move(vec);
        }

        ui32 GetLsnRange() const {
            ui32 res = 0;
            for (const auto& x : LogoBlobsAdditions) {
                res += x.RecsNum;
            }
            for (const auto& x : BlocksAdditions) {
                res += x.RecsNum;
            }
            for (const auto& x : BarriersAdditions) {
                res += x.RecsNum;
            }
            return res;
        }

    private:
        template <class TAddition>
        static void SerializeToProto(TAddition &x, NKikimrVDiskData::TAddBulkSstRecoveryLogRec::TSstAndRecsNum *ad) {
            x.Sst->SerializeToProto(*ad->MutableSst());
            ad->SetRecsNum(x.RecsNum);
        }
    };

    template <>
    inline TAddBulkSstEssence::TSstAndRecsNum<TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>>
    TAddBulkSstEssence::EnsureOnlyOneSst<TKeyLogoBlob, TMemRecLogoBlob>() const {
        Y_ABORT_UNLESS(BlocksAdditions.empty() && BarriersAdditions.empty() && LogoBlobsAdditions.size() == 1);
        const auto &addition = LogoBlobsAdditions.at(0);
        return addition;
    }

    template <>
    inline TAddBulkSstEssence::TSstAndRecsNum<TLevelSegment<TKeyBlock, TMemRecBlock>>
    TAddBulkSstEssence::EnsureOnlyOneSst<TKeyBlock, TMemRecBlock>() const {
        Y_ABORT("Must not be called");
    }

    template <>
    inline TAddBulkSstEssence::TSstAndRecsNum<TLevelSegment<TKeyBarrier, TMemRecBarrier>>
    TAddBulkSstEssence::EnsureOnlyOneSst<TKeyBarrier, TMemRecBarrier>() const {
        Y_ABORT("Must not be called");
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TEvAddBulkSst
    // Atomically add already built SSTables to the Hull Database
    ////////////////////////////////////////////////////////////////////////////////
    struct TEvAddBulkSst : public TEventLocal<TEvAddBulkSst, TEvBlobStorage::EvAddBulkSst> {
        // unused chunks left after building SSTables
        TVector<ui32> ReservedChunks;
        // vector of chunk ID's to commit into pdisk
        TVector<ui32> ChunksToCommit;
        // SSTables to add
        TAddBulkSstEssence Essence;
        // actor ID to notify after commit
        TActorId NotifyId;

        TEvAddBulkSst() = default;

        TEvAddBulkSst(TVector<ui32>&& reservedChunks, TVector<ui32>&& chunksToCommit,
                TLogoBlobsSstPtr &&oneLogoBlobsSst, ui32 oneLogoBlobsSstRecsNum);

        // returns message for writing to recovery log and and fill in commitRecord
        TString Serialize(NPDisk::TCommitRecord &commitRecord);
    };

    ////////////////////////////////////////////////////////////////////////////////
    // TEvAddBulkSstResult - confirmation on TEvAddBulkSst
    ////////////////////////////////////////////////////////////////////////////////
    struct TEvAddBulkSstResult : public TEventLocal<TEvAddBulkSstResult, TEvBlobStorage::EvAddBulkSstResult> {
    };

} // NKikimr

