#pragma once

#include "defs.h"
#include "flat_part_scheme.h"
#include "flat_page_btree_index.h"
#include "flat_page_flat_index.h"
#include "flat_page_data.h"
#include "flat_page_blobs.h"
#include "flat_page_frames.h"
#include "flat_page_bloom.h"
#include "flat_page_gstat.h"
#include "flat_page_txidstat.h"
#include "flat_page_txstatus.h"

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

        struct TIndexPages {
            using TPageId = NPage::TPageId;
            using TGroupId = NPage::TGroupId;
            using TBtreeIndexMeta = NPage::TBtreeIndexMeta;
            const TVector<TPageId> FlatGroups;
            const TVector<TPageId> FlatHistoric;
            const TVector<TBtreeIndexMeta> BTreeGroups;
            const TVector<TBtreeIndexMeta> BTreeHistoric;

            bool HasBTree() const noexcept {
                return !BTreeGroups.empty();
            }

            bool HasFlat() const noexcept {
                return !FlatGroups.empty();
            }

            const TBtreeIndexMeta& GetBTree(TGroupId groupId) const noexcept {
                if (groupId.IsHistoric()) {
                    Y_ABORT_UNLESS(groupId.Index < BTreeHistoric.size());
                    return BTreeHistoric[groupId.Index];
                } else {
                    Y_ABORT_UNLESS(groupId.Index < BTreeGroups.size());
                    return BTreeGroups[groupId.Index];
                }
            }

            TPageId GetFlat(TGroupId groupId) const noexcept {
                if (groupId.IsHistoric()) {
                    Y_ABORT_UNLESS(groupId.Index < FlatHistoric.size());
                    return FlatHistoric[groupId.Index];
                } else {
                    Y_ABORT_UNLESS(groupId.Index < FlatGroups.size());
                    return FlatGroups[groupId.Index];
                }
            }
        };

        struct TParams {
            TEpoch Epoch;
            TIntrusiveConstPtr<TPartScheme> Scheme;
            TIndexPages IndexPages;
            TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
            TIntrusiveConstPtr<NPage::TBloom> ByKey;
            TIntrusiveConstPtr<NPage::TFrames> Large;
            TIntrusiveConstPtr<NPage::TFrames> Small;
            size_t IndexesRawSize;
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
            , IndexPages(std::move(params.IndexPages))
            , ByKey(std::move(params.ByKey))
            , GarbageStats(std::move(params.GarbageStats))
            , TxIdStats(std::move(params.TxIdStats))
            , Stat(stat)
            , GroupsCount(Max(IndexPages.FlatGroups.size(), IndexPages.BTreeGroups.size()))
            , HistoricGroupsCount(Max(IndexPages.FlatHistoric.size(), IndexPages.BTreeHistoric.size()))
            , IndexesRawSize(params.IndexesRawSize)
            , MinRowVersion(params.MinRowVersion)
            , MaxRowVersion(params.MaxRowVersion)
        {
            Y_ABORT_UNLESS(Scheme->Groups.size() == GroupsCount,
                "Part has scheme with %" PRISZT " groups, but %" PRISZT " indexes",
                Scheme->Groups.size(), GroupsCount);
            Y_ABORT_UNLESS(!HistoricGroupsCount || HistoricGroupsCount == GroupsCount,
                "Part has %" PRISZT " indexes, but %" PRISZT " historic indexes",
                GroupsCount, HistoricGroupsCount);
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
        virtual ui64 GetPageSize(NPage::TPageId pageId, NPage::TGroupId groupId) const = 0;
        virtual ui64 GetPageSize(ELargeObj lob, ui64 ref) const = 0;
        virtual NPage::EPage GetPageType(NPage::TPageId pageId, NPage::TGroupId groupId) const = 0;
        virtual ui8 GetGroupChannel(NPage::TGroupId groupId) const = 0;
        virtual ui8 GetPageChannel(ELargeObj lob, ui64 ref) const = 0;

    protected:
        // Helper for CloneWithEpoch
        TPart(const TPart& src, TEpoch epoch)
            : Label(src.Label)
            , Epoch(epoch)
            , Scheme(src.Scheme)
            , Blobs(src.Blobs)
            , Large(src.Large)
            , Small(src.Small)
            , IndexPages(src.IndexPages)
            , ByKey(src.ByKey)
            , GarbageStats(src.GarbageStats)
            , Stat(src.Stat)
            , GroupsCount(src.GroupsCount)
            , HistoricGroupsCount(src.HistoricGroupsCount)
            , IndexesRawSize(src.IndexesRawSize)
            , MinRowVersion(src.MinRowVersion)
            , MaxRowVersion(src.MaxRowVersion)
        { }

    public:
        const TLogoBlobID Label;
        const TEpoch Epoch;
        const TIntrusiveConstPtr<TPartScheme> Scheme;
        const TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
        const TIntrusiveConstPtr<NPage::TFrames> Large;
        const TIntrusiveConstPtr<NPage::TFrames> Small;
        const TIndexPages IndexPages;
        const TIntrusiveConstPtr<NPage::TBloom> ByKey;
        const TIntrusiveConstPtr<NPage::TGarbageStats> GarbageStats;
        const TIntrusiveConstPtr<NPage::TTxIdStatsPage> TxIdStats;
        const TStat Stat;
        const size_t GroupsCount;
        const size_t HistoricGroupsCount;
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
