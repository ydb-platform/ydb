#pragma once

#include "defs.h"
#include "flat_part_scheme.h"
#include "flat_page_btree_index.h"
#include "flat_page_flat_index.h"
#include "flat_page_data.h"
#include "flat_page_blobs.h"
#include "flat_page_frames.h"
#include "flat_page_bloom.h"
#include "flat_bloom_hash.h"
#include "flat_page_gstat.h"
#include "flat_page_txidstat.h"
#include "flat_page_txstatus.h"

#include <type_traits>
#include <variant>

namespace NKikimr {
namespace NTable {
    struct IPages;

    /// Resolves the root TPageLocation from btree meta through page collections.
    /// For v2 roots (byte-offset), returns the inline location directly.
    /// For v1 roots (page-index), resolves through the appropriate page collection.
    /// When LevelCount==0 the root IS a data page (use groupPages);
    /// when LevelCount>0 the root is a btree index page (use indexPages).
    inline NPage::TPageLocation GetBTreeRootLocation(
        const NPage::TBtreeIndexMeta& meta,
        const NPageCollection::IPageCollection* indexPages,
        const NPageCollection::IPageCollection* groupPages)
    {
        if (meta.HasRootV2()) return meta.RootV2;
        return (meta.LevelCount() == 0 ? *groupPages : *indexPages).GetLocation(meta.RootV1PageId());
    }

    using TPageRef = NPage::TPageRef;

    /**
     * Maps kinds of part pages to page collections (slots) of the part
     *
     * Shared by the part and its loader, which needs it before the part is created
     */
    class TPartSlots {
    public:
        explicit TPartSlots(ui32 groupsCount) noexcept
            : GroupsCount(groupsCount)
        { }

        /**
         * Slot with the part root (Schem2), bloom filters, frames, globs and stats pages
         */
        ui32 DescriptorSlot() const noexcept
        {
            return 0;
        }

        /**
         * Slot with data pages of the column group, historic data pages share the slot of their group
         */
        ui32 DataSlot(NPage::TGroupId groupId) const noexcept
        {
            return groupId.Index;
        }

        /**
         * Slot with index pages (flat or b-tree) of the column group, historic ones included
         */
        ui32 IndexSlot(NPage::TGroupId groupId) const noexcept
        {
            Y_UNUSED(groupId);
            return 0;
        }

        /**
         * Slot with outer (packed) values
         */
        ui32 OuterSlot() const noexcept
        {
            return GroupsCount;
        }

    private:
        ui32 GroupsCount;
    };

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

        void Describe(IOutputStream &out) const
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

            const TBtreeIndexMeta& GetBTree(TGroupId groupId) const {
                if (groupId.IsHistoric()) {
                    Y_ENSURE(groupId.Index < BTreeHistoric.size());
                    return BTreeHistoric[groupId.Index];
                } else {
                    Y_ENSURE(groupId.Index < BTreeGroups.size());
                    return BTreeGroups[groupId.Index];
                }
            }

            TPageId GetFlat(TGroupId groupId) const {
                if (groupId.IsHistoric()) {
                    Y_ENSURE(groupId.Index < FlatHistoric.size());
                    return FlatHistoric[groupId.Index];
                } else {
                    Y_ENSURE(groupId.Index < FlatGroups.size());
                    return FlatGroups[groupId.Index];
                }
            }

            /// Resolves the root TPageLocation from the btree meta through the part's page collections.
            /// When LevelCount==0 the root IS a data page of the group,
            /// when LevelCount>0 the root is a btree index page of the group.
            using TMetaRef = NPage::TBtreeIndexMeta;
            using TPages = NPageCollection::IPageCollection;
            NPage::TPageLocation GetRootLocation(const TPart* part, TGroupId groupId) const {
                const auto& meta = GetBTree(groupId);
                return ::NKikimr::NTable::GetBTreeRootLocation(meta,
                    part->GetIndexPageCollection(groupId),
                    part->GetDataPageCollection(groupId));
            }
        };

        struct TParams {
            TEpoch Epoch;
            TIntrusiveConstPtr<TPartScheme> Scheme;
            TIndexPages IndexPages;
            TIntrusiveConstPtr<NPage::TExtBlobs> Blobs;
            TVector<std::pair<ui32, TIntrusiveConstPtr<NPage::TBloom>>> ByKeyPrefixes;
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
            , ByKeyPrefixes(std::move(params.ByKeyPrefixes))
            , GarbageStats(std::move(params.GarbageStats))
            , TxIdStats(std::move(params.TxIdStats))
            , Stat(stat)
            , GroupsCount(Max(IndexPages.FlatGroups.size(), IndexPages.BTreeGroups.size()))
            , HistoricGroupsCount(Max(IndexPages.FlatHistoric.size(), IndexPages.BTreeHistoric.size()))
            , Slots(GroupsCount)
            , IndexesRawSize(params.IndexesRawSize)
            , MinRowVersion(params.MinRowVersion)
            , MaxRowVersion(params.MaxRowVersion)
        {
            Y_ENSURE(Scheme->Groups.size() == GroupsCount,
                "Part has scheme with " << Scheme->Groups.size() << " groups, but " << GroupsCount << " indexes");
            Y_ENSURE(!HistoricGroupsCount || HistoricGroupsCount == GroupsCount,
                "Part has " << GroupsCount << " indexes, but " << HistoricGroupsCount << " historic indexes");
            Y_ENSURE(std::is_sorted(ByKeyPrefixes.begin(), ByKeyPrefixes.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; }),
                "ByKeyPrefixes must be sorted by prefix length");
        }

        virtual ~TPart() = default;

        void Describe(IOutputStream &out) const
        {
            out
                << "Part{" << Label << " eph " << Epoch << ", "
                << Stat.Coded << "b " << Stat.Rows << "r}";
        }

        bool MightHaveKeyPrefix(const NBloom::TPrefix& prefix) const
        {
            // Check from the longest (most selective) prefix first
            for (auto it = ByKeyPrefixes.rbegin(); it != ByKeyPrefixes.rend(); ++it) {
                const auto& [prefixLen, bloom] = *it;
                if (bloom && prefixLen > 0) {
                    if (!bloom->MightHave(prefix.Get(prefixLen))) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Returns a cloned part with Epoch changed to the specified epoch
         */
        virtual TIntrusiveConstPtr<TPart> CloneWithEpoch(TEpoch epoch) const = 0;

        virtual ui64 DataSize() const noexcept = 0;
        virtual ui64 BackingSize() const noexcept = 0;
        virtual ui64 GetPageSize(ELargeObj lob, ui64 ref) const = 0;
        virtual ui8 GetPageChannel(ELargeObj lob, ui64 ref) const = 0;

        /**
         * Page collections of the part addressed by slot, see TPartSlots
         */
        virtual const NPageCollection::IPageCollection* GetPageCollection(ui32 slot) const = 0;
        virtual ui64 GetSlotPageSize(ui32 slot, NPage::TPageId pageId) const = 0;
        virtual NPage::EPage GetSlotPageType(ui32 slot, NPage::TPageId pageId) const = 0;
        virtual ui8 GetSlotChannel(ui32 slot) const = 0;

        ui32 DescriptorSlot() const noexcept
        {
            return Slots.DescriptorSlot();
        }

        ui32 DataSlot(NPage::TGroupId groupId) const noexcept
        {
            return Slots.DataSlot(groupId);
        }

        ui32 IndexSlot(NPage::TGroupId groupId) const noexcept
        {
            return Slots.IndexSlot(groupId);
        }

        ui32 OuterSlot() const noexcept
        {
            return Slots.OuterSlot();
        }

        /**
         * Data pages of the column group
         */
        ui64 GetPageSize(NPage::TPageId pageId, NPage::TGroupId groupId) const
        {
            return GetSlotPageSize(DataSlot(groupId), pageId);
        }

        NPage::EPage GetPageType(NPage::TPageId pageId, NPage::TGroupId groupId) const
        {
            return GetSlotPageType(DataSlot(groupId), pageId);
        }

        NPage::TPageLocation GetPageLocation(NPage::TPageId pageId, NPage::TGroupId groupId) const
        {
            return GetDataPageCollection(groupId)->GetLocation(pageId);
        }

        const NPageCollection::IPageCollection* GetDataPageCollection(NPage::TGroupId groupId) const
        {
            return GetPageCollection(DataSlot(groupId));
        }

        ui8 GetDataChannel(NPage::TGroupId groupId) const
        {
            return GetSlotChannel(DataSlot(groupId));
        }

        /**
         * Index pages (flat or b-tree) of the column group
         */
        NPage::EPage GetIndexPageType(NPage::TPageId pageId, NPage::TGroupId groupId) const
        {
            return GetSlotPageType(IndexSlot(groupId), pageId);
        }

        NPage::TPageLocation GetIndexPageLocation(NPage::TPageId pageId, NPage::TGroupId groupId) const
        {
            return GetIndexPageCollection(groupId)->GetLocation(pageId);
        }

        const NPageCollection::IPageCollection* GetIndexPageCollection(NPage::TGroupId groupId) const
        {
            return GetPageCollection(IndexSlot(groupId));
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
            , IndexPages(src.IndexPages)
            , ByKeyPrefixes(src.ByKeyPrefixes)
            , GarbageStats(src.GarbageStats)
            , Stat(src.Stat)
            , GroupsCount(src.GroupsCount)
            , HistoricGroupsCount(src.HistoricGroupsCount)
            , Slots(src.Slots)
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
        const TVector<std::pair<ui32, TIntrusiveConstPtr<NPage::TBloom>>> ByKeyPrefixes;
        const TIntrusiveConstPtr<NPage::TGarbageStats> GarbageStats;
        const TIntrusiveConstPtr<NPage::TTxIdStatsPage> TxIdStats;
        const TStat Stat;
        const size_t GroupsCount;
        const size_t HistoricGroupsCount;
        const TPartSlots Slots;
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

        void Describe(IOutputStream &out) const
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

    /// Resolve a TPageRef of a data page to a concrete TPageLocation through the part's page collections.
    inline NPage::TPageLocation ResolvePageLocation(const TPart* part, const TPageRef& ref, NPage::TGroupId groupId) {
        return std::visit([&](const auto& val) -> NPage::TPageLocation {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, NPage::TPageId>) {
                return part->GetPageLocation(val, groupId);
            } else {
                return val;
            }
        }, ref);
    }

    /// Resolve a TPageRef of an index page to a concrete TPageLocation through the part's page collections.
    inline NPage::TPageLocation ResolveIndexPageLocation(const TPart* part, const TPageRef& ref, NPage::TGroupId groupId) {
        return std::visit([&](const auto& val) -> NPage::TPageLocation {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, NPage::TPageId>) {
                return part->GetIndexPageLocation(val, groupId);
            } else {
                return val;
            }
        }, ref);
    }

}
}
