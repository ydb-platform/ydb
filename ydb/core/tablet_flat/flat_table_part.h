#pragma once

#include "defs.h"
#include "flat_part_scheme.h"
#include "flat_page_index.h"
#include "flat_page_data.h"
#include "flat_page_blobs.h"
#include "flat_page_frames.h"
#include "flat_page_bloom.h"
#include "flat_page_gstat.h"
#include "flat_page_txidstat.h"
#include "flat_page_txstatus.h"
#include "util_basics.h"

namespace NKikimr {
namespace NTable {
    struct IPages;

    /**
     * Cold parts are parts that don't have any metadata loaded into memory,
     * so we don't know much about them. Concrete implementations should
     * contain everything needed to load and turn them into real parts.
     */
    class TColdPart : public virtual TThrRefBase {
    public:
        TColdPart(const TLogoBlobID &label, TEpoch epoch)
            : Label(label)
            , Epoch(epoch)
        {}

        virtual ~TColdPart() = default;

        void Describe(IOutputStream &out) const noexcept
        {
            out << "ColdPart{" << Label << " eph " << Epoch << "}";
        }

    public:
        const TLogoBlobID Label;
        const TEpoch Epoch;
    };

    class TPart : public virtual TThrRefBase {
    public:
        enum ELimits : ui32 {
            Trace = 2, /* how many last data pages to keep while seq scans */
        };

        struct TParams {
            TEpoch Epoch;
            TIntrusiveConstPtr<TPartScheme> Scheme;
            TSharedData Index;
            TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
            TIntrusiveConstPtr<NPage::TBloom> ByKey;
            TIntrusiveConstPtr<NPage::TFrames> Large;
            TIntrusiveConstPtr<NPage::TFrames> Small;
            TVector<TSharedData> GroupIndexes;
            TVector<TSharedData> HistoricIndexes;
            TRowVersion MinRowVersion;
            TRowVersion MaxRowVersion;
            TIntrusiveConstPtr<NPage::TGarbageStats> GarbageStats;
            TIntrusiveConstPtr<NPage::TTxIdStatsPage> TxIdStats;
        };

        struct TStat {
            ui64 Bytes;     /* Part raw data (unencoded) bytes  */
            ui64 Coded;     /* Encoded data pages in part bytes */
            ui64 Drops;     /* Total rows with ERowOp::Erase code */
            ui64 Rows;      /* Total rows count in the TPart    */
            ui64 HiddenRows; /* Hidden (non-main) total rows */
            ui64 HiddenDrops; /* Hidden (non-main) rows with ERowOp::Erase */
        };

        TPart(const TLogoBlobID &label, TParams params, TStat stat)
            : Label(label)
            , Epoch(params.Epoch)
            , Scheme(std::move(params.Scheme))
            , Blobs(std::move(params.Blobs))
            , Large(std::move(params.Large))
            , Small(std::move(params.Small))
            , Index(std::move(params.Index))
            , GroupIndexes(
                std::make_move_iterator(params.GroupIndexes.begin()),
                std::make_move_iterator(params.GroupIndexes.end()))
            , HistoricIndexes(
                std::make_move_iterator(params.HistoricIndexes.begin()),
                std::make_move_iterator(params.HistoricIndexes.end()))
            , ByKey(std::move(params.ByKey))
            , GarbageStats(std::move(params.GarbageStats))
            , TxIdStats(std::move(params.TxIdStats))
            , Stat(stat)
            , Groups(1 + GroupIndexes.size())
            , IndexesRawSize(Index.RawSize() + SumRawSize(GroupIndexes))
            , MinRowVersion(params.MinRowVersion)
            , MaxRowVersion(params.MaxRowVersion)
        {
            Y_VERIFY(Scheme->Groups.size() == Groups,
                "Part has scheme with %" PRISZT " groups, but %" PRISZT " indexes",
                Scheme->Groups.size(), Groups);
            Y_VERIFY(HistoricIndexes.empty() || HistoricIndexes.size() == Groups,
                "Part has %" PRISZT " indexes, but %" PRISZT " historic indexes",
                Groups, HistoricIndexes.size());
        }

        virtual ~TPart() = default;

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Part{" << Label << " eph " << Epoch << ", "
                << Stat.Coded << "b " << Stat.Rows << "r}";
        }

        bool MightHaveKey(TStringBuf serializedKey) const
        {
            return ByKey ? ByKey->MightHave(serializedKey) : true;
        }

        /**
         * Returns a cloned part with Epoch changed to the specified epoch
         */
        virtual TIntrusiveConstPtr<TPart> CloneWithEpoch(TEpoch epoch) const = 0;

        virtual ui64 DataSize() const = 0;
        virtual ui64 BackingSize() const = 0;
        virtual ui64 GetPageSize(NPage::TPageId id, NPage::TGroupId groupId = { }) const = 0;
        virtual ui8 GetPageChannel(NPage::TPageId id, NPage::TGroupId groupId = { }) const = 0;
        virtual ui8 GetPageChannel(ELargeObj lob, ui64 ref) const = 0;

        const NPage::TIndex& GetGroupIndex(NPage::TGroupId groupId) const noexcept {
            if (!groupId.Historic) {
                if (groupId.Index == 0) {
                    return Index;
                } else {
                    Y_VERIFY(groupId.Index <= GroupIndexes.size(),
                        "Group index %" PRIu32 " is missing",
                        groupId.Index);
                    return GroupIndexes[groupId.Index - 1];
                }
            } else {
                Y_VERIFY(groupId.Index < HistoricIndexes.size(),
                    "Historic index %" PRIu32 " is missing",
                    groupId.Index);
                return HistoricIndexes[groupId.Index];
            }
        }

    protected:
        // Helper for CloneWithEpoch
        TPart(const TPart& src, TEpoch epoch)
            : Label(src.Label)
            , Epoch(epoch)
            , Scheme(src.Scheme)
            , Blobs(src.Blobs)
            , Large(src.Large)
            , Small(src.Small)
            , Index(src.Index)
            , GroupIndexes(src.GroupIndexes)
            , HistoricIndexes(src.HistoricIndexes)
            , ByKey(src.ByKey)
            , GarbageStats(src.GarbageStats)
            , Stat(src.Stat)
            , Groups(src.Groups)
            , IndexesRawSize(src.IndexesRawSize)
            , MinRowVersion(src.MinRowVersion)
            , MaxRowVersion(src.MaxRowVersion)
        { }

    private:
        static size_t SumRawSize(const TVector<NPage::TIndex>& indexes) {
            size_t ret = 0;
            for (auto& index : indexes) {
                ret += index.RawSize();
            }
            return ret;
        }

    public:
        const TLogoBlobID Label;
        const TEpoch Epoch;
        const TIntrusiveConstPtr<TPartScheme> Scheme;
        const TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
        const TIntrusiveConstPtr<NPage::TFrames> Large;
        const TIntrusiveConstPtr<NPage::TFrames> Small;
        const NPage::TIndex Index;
        const TVector<NPage::TIndex> GroupIndexes;
        const TVector<NPage::TIndex> HistoricIndexes;
        const TIntrusiveConstPtr<NPage::TBloom> ByKey;
        const TIntrusiveConstPtr<NPage::TGarbageStats> GarbageStats;
        const TIntrusiveConstPtr<NPage::TTxIdStatsPage> TxIdStats;
        const TStat Stat;
        const size_t Groups;
        const size_t IndexesRawSize;
        const TRowVersion MinRowVersion;
        const TRowVersion MaxRowVersion;
    };

    /**
     * This class represents a loaded part of tx status table, identified by its label
     */
    class TTxStatusPart : public virtual TThrRefBase {
    public:
        TTxStatusPart(const TLogoBlobID &label, TEpoch epoch, TIntrusiveConstPtr<NPage::TTxStatusPage> txStatusPage)
            : Label(label)
            , Epoch(epoch)
            , TxStatusPage(std::move(txStatusPage))
        { }

        virtual ~TTxStatusPart() = default;

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "TxStatus{" << Label << " epoch " << Epoch << ", "
                << TxStatusPage->GetCommittedItems().size() << " committed,"
                << TxStatusPage->GetRemovedItems().size() << " removed}";
        }

    public:
        const TLogoBlobID Label;
        const TEpoch Epoch;
        const TIntrusiveConstPtr<NPage::TTxStatusPage> TxStatusPage;
    };

}
}
