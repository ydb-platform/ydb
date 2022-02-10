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
            const auto got = NPage::THello().Read(Raw, EPage::GarbageStats);

            Y_VERIFY(got == ECodec::Plain && got.Version == 0);

            Y_VERIFY(sizeof(THeader) <= got.Page.size(),
                    "NPage::TGarbageStats header is out of blob bounds");

            auto* header = TDeref<THeader>::At(got.Page.data(), 0);

            Y_VERIFY(header->Type == 0,
                    "NPage::TGarbageStats header has an unsupported type");

            Y_VERIFY(sizeof(THeader) + header->Items * sizeof(TItem) <= got.Page.size(),
                    "NPage::TGarbageStats items are out of blob bounds");

            auto* ptr = TDeref<TItem>::At(got.Page.data(), sizeof(THeader));

            Items = { ptr, ptr + header->Items };
        }

        ui64 Count() const {
            return Items.size();
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
            Y_VERIFY_DEBUG(it != Entries.end());
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
                Y_VERIFY_DEBUG(IsLast(it));
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
            Y_VERIFY_DEBUG(!IsLast(it));
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
                Y_VERIFY_DEBUG(it != Entries.end());
                auto next = std::next(it);
                Y_VERIFY_DEBUG(next != Entries.end());
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

            if (auto* label = out.Skip<NPage::TLabel>()) {
                label->Init(EPage::GarbageStats, 0, pageSize);
            }

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

            Y_VERIFY(*out == buf.mutable_end());
            NSan::CheckMemIsInitialized(buf.data(), buf.size());

            return buf;
        }

    private:
        TEntries Entries;
        THeapByBytes ByBytes;
    };

}   // namespace NPage
}   // namespace NTable
}   // namespace NKikimr
