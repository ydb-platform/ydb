#pragma once

#include "flat_page_label.h"
#include "flat_util_binary.h"

#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/map.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    class TGarbageStats : public TThrRefBase {
    public:
        struct THeader {
            ui16 Type = 0;
            ui16 Pad0_ = 0;
            ui32 Pad1_ = 0;
            ui64 Items = 0;
        } Y_PACKED;

        struct TItem {
            ui64 Step_;
            ui64 TxId_;
            ui64 Bytes_;

            TRowVersion GetRowVersion() const {
                return TRowVersion(Step_, TxId_);
            }

            ui64 GetBytes() const {
                return Bytes_;
            }
        } Y_PACKED;

        static_assert(sizeof(THeader) == 16, "Invalid THeader size");
        static_assert(sizeof(TItem) == 24, "Invalid TItem size");

    public:
        TGarbageStats(TSharedData page)
            : Raw(std::move(page))
        {
            const auto got = NPage::TLabelWrapper().Read(Raw, EPage::GarbageStats);

            Y_ABORT_UNLESS(got == ECodec::Plain && got.Version == 0);

            Y_ABORT_UNLESS(sizeof(THeader) <= got.Page.size(),
                    "NPage::TGarbageStats header is out of blob bounds");

            auto* header = TDeref<THeader>::At(got.Page.data(), 0);

            Y_ABORT_UNLESS(header->Type == 0,
                    "NPage::TGarbageStats header has an unsupported type");

            Y_ABORT_UNLESS(sizeof(THeader) + header->Items * sizeof(TItem) <= got.Page.size(),
                    "NPage::TGarbageStats items are out of blob bounds");

            auto* ptr = TDeref<TItem>::At(got.Page.data(), sizeof(THeader));

            Items = { ptr, ptr + header->Items };
        }

        size_t Count() const {
            return Items.size();
        }

        TRowVersion GetRowVersionAtIndex(size_t index) const {
            return Items[index].GetRowVersion();
        }

        ui64 GetGarbageBytesAtIndex(size_t index) const {
            return Items[index].GetBytes();
        }

        /**
         * Returns number of bytes that are guaranteed to be freed
         * if everything up to rowVersion is marked as removed.
         */
        ui64 GetGarbageBytes(const TRowVersion& rowVersion) const {
            if (!Items) {
                return 0;
            }

            auto cmp = [](const TRowVersion& rowVersion, const TItem& item) -> bool {
                return rowVersion < item.GetRowVersion();
            };

            // First item with version > rowVersion
            auto it = std::upper_bound(Items.begin(), Items.end(), rowVersion, cmp);
            if (it == Items.begin()) {
                return 0;
            }

            // First item with version <= rowVersion
            return (--it)->GetBytes();
        }

    private:
        TSharedData Raw;
        TArrayRef<const TItem> Items;
    };

    class TGarbageStatsBuilder {
        struct TValue {
            ui64 Bytes = 0;
            size_t HeapIndex = -1;

            explicit TValue(ui64 bytes)
                : Bytes(bytes)
            { }
        };

        using TEntries = TMap<TRowVersion, TValue>;
        using TEntry = TEntries::value_type;

        struct TCompareByBytes {
            bool operator()(const TEntry& a, const TEntry& b) const {
                return a.second.Bytes < b.second.Bytes;
            }
        };

        struct THeapIndexByBytes {
            size_t& operator()(TEntry& entry) const {
                return entry.second.HeapIndex;
            }
        };

        using THeapByBytes = TIntrusiveHeap<TEntry, THeapIndexByBytes, TCompareByBytes>;

        bool IsLast(TEntries::iterator it) {
            Y_DEBUG_ABORT_UNLESS(it != Entries.end());
            return ++it == Entries.end();
        }

    public:
        void Add(const TRowVersion& rowVersion, ui64 bytes) {
            auto it = Entries.lower_bound(rowVersion);
            if (it == Entries.end()) {
                // We are adding a new last entry, previous entry must go to heap
                if (!Entries.empty()) {
                    auto prev = std::prev(it);
                    ByBytes.Add(&*prev);
                }
                // Last entry is never on the heap
                it = Entries.emplace_hint(
                    it, std::piecewise_construct,
                    std::forward_as_tuple(rowVersion),
                    std::forward_as_tuple(bytes));
                Y_DEBUG_ABORT_UNLESS(IsLast(it));
                return;
            }

            if (it->first == rowVersion) {
                // Update an existing entry
                it->second.Bytes += bytes;
                if (!IsLast(it)) {
                    ByBytes.Update(&*it);
                }
                return;
            }

            // Insert a new entry and add to heap
            it = Entries.emplace_hint(
                it, std::piecewise_construct,
                std::forward_as_tuple(rowVersion),
                std::forward_as_tuple(bytes));
            Y_DEBUG_ABORT_UNLESS(!IsLast(it));
            ByBytes.Add(&*it);
        }

        explicit operator bool() const {
            return !Entries.empty();
        }

        size_t Size() const {
            return Entries.size();
        }

        void ShrinkTo(size_t count) {
            TEntry* top;
            while (Entries.size() > count && (top = ByBytes.Top())) {
                auto it = Entries.find(top->first);
                Y_DEBUG_ABORT_UNLESS(it != Entries.end());
                auto next = std::next(it);
                Y_DEBUG_ABORT_UNLESS(next != Entries.end());
                ByBytes.Remove(top);
                next->second.Bytes += top->second.Bytes;
                if (!IsLast(next)) {
                    ByBytes.Update(&*next);
                }
                Entries.erase(it);
            }
        }

        TSharedData Finish() const {
            if (!Entries) {
                return { };
            }

            using THeader = TGarbageStats::THeader;
            using TItem = TGarbageStats::TItem;

            size_t pageSize = (
                    sizeof(NPage::TLabel) +
                    sizeof(THeader) +
                    sizeof(TItem) * Entries.size());

            TSharedData buf = TSharedData::Uninitialized(pageSize);

            NUtil::NBin::TPut out(buf.mutable_begin());

            WriteUnaligned<TLabel>(out.Skip<TLabel>(), TLabel::Encode(EPage::GarbageStats, 0, pageSize));

            if (auto* header = out.Skip<THeader>()) {
                Zero(*header);
                header->Type = 0;
                header->Items = Entries.size();
            }

            ui64 totalGarbage = 0;
            for (const auto& entry : Entries) {
                totalGarbage += entry.second.Bytes;
                auto* item = out.Skip<TItem>();
                item->Step_ = entry.first.Step;
                item->TxId_ = entry.first.TxId;
                item->Bytes_ = totalGarbage;
            }

            Y_ABORT_UNLESS(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            return buf;
        }

    private:
        TEntries Entries;
        THeapByBytes ByBytes;
    };

    /**
     * An aggregate across multiple TGarbageStats pages, reads are O(log N)
     */
    class TGarbageStatsAgg {
    private:
        friend class TGarbageStatsAggBuilder;

        struct TItem {
            ui64 Step;
            ui64 TxId;
            ui64 Bytes;

            TRowVersion GetRowVersion() const {
                return TRowVersion(Step, TxId);
            }

            ui64 GetBytes() const {
                return Bytes;
            }
        };

    public:
        TGarbageStatsAgg() = default;

        TGarbageStatsAgg(const TGarbageStatsAgg&) = delete;
        TGarbageStatsAgg& operator=(const TGarbageStatsAgg&) = delete;

        TGarbageStatsAgg(TGarbageStatsAgg&&) noexcept = default;
        TGarbageStatsAgg& operator=(TGarbageStatsAgg&&) noexcept = default;

    private:
        TGarbageStatsAgg(TVector<TItem>&& items)
            : Items(std::move(items))
        { }

    public:
        /**
         * Returns number of bytes that are guaranteed to be freed
         * if everything up to rowVersion is marked as removed.
         */
        ui64 GetGarbageBytes(const TRowVersion& rowVersion) const {
            if (Items.empty()) {
                return 0;
            }

            auto cmp = [](const TRowVersion& rowVersion, const TItem& item) -> bool {
                return rowVersion < item.GetRowVersion();
            };

            // First item with version > rowVersion
            auto it = std::upper_bound(Items.begin(), Items.end(), rowVersion, cmp);
            if (it == Items.begin()) {
                return 0;
            }

            // First item with version <= rowVersion
            return (--it)->GetBytes();
        }

    private:
        TVector<TItem> Items;
    };

    class TGarbageStatsAggBuilder {
    private:
        using TItem = TGarbageStatsAgg::TItem;

    public:
        TGarbageStatsAggBuilder() = default;

        /**
         * Adds a delta bytes that is guaranteed to be freed when everything
         * up to rowVersion is marked as removed.
         */
        void Add(const TRowVersion& rowVersion, ui64 bytes) {
            Items.push_back(TItem{ rowVersion.Step, rowVersion.TxId, bytes });
        }

        /**
         * Adds all data in the given garbage stats page to the aggregate.
         */
        void Add(const TGarbageStats* stats) {
            ui64 prev = 0;
            size_t count = stats->Count();
            for (size_t index = 0; index < count; ++index) {
                TRowVersion rowVersion = stats->GetRowVersionAtIndex(index);
                ui64 bytes = stats->GetGarbageBytesAtIndex(index);
                Add(rowVersion, bytes - prev);
                prev = bytes;
            }
        }

        /**
         * Adds all data in the given garbage stats page to the aggregate.
         */
        void Add(const TIntrusiveConstPtr<TGarbageStats>& stats) {
            Add(stats.Get());
        }

        /**
         * Builds an aggregate object, destroying builder in the process
         */
        TGarbageStatsAgg Build() {
            if (!Items.empty()) {
                auto cmp = [](const TItem& a, const TItem& b) -> bool {
                    return a.GetRowVersion() < b.GetRowVersion();
                };

                // Sort data by row version
                std::sort(Items.begin(), Items.end(), cmp);

                // Compute aggregate bytes
                ui64 bytes = 0;
                auto dst = Items.begin();
                for (auto src = Items.begin(); src != Items.end(); ++src) {
                    bytes += src->GetBytes();
                    if (dst != Items.begin()) {
                        auto last = std::prev(dst);
                        if (last->GetRowVersion() == src->GetRowVersion()) {
                            last->Bytes = bytes;
                            continue;
                        }
                    }
                    if (dst != src) {
                        *dst = *src;
                    }
                    dst->Bytes = bytes;
                    ++dst;
                }

                if (dst != Items.end()) {
                    Items.erase(dst, Items.end());
                }

                Items.shrink_to_fit();
            }

            TGarbageStatsAgg agg(std::move(Items));
            Items.clear();
            return agg;
        }

    private:
        TVector<TItem> Items;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
