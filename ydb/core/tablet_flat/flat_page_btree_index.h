#pragma once

#include <ydb/core/base/defs.h>
#include <util/generic/bitmap.h>
#include "flat_page_base.h"
#include "flat_page_label.h"

namespace NKikimr::NTable::NPage {
    
    /*
        TKey binary layout
        .-------------------------.
        | Non-null cell bitmap    | for all schema key columns
        .-------------------------.      -.
        | value OR offs           | col_1 |
        .-------------------------.       |
        |       .    .    .       |       | for each non-null column
        .-------------------------.       |
        | value OR offs           | col_K |
        .-.------.--.-------.-----.      -'
        | |      |  |       |     | var-size values
        '-'------'--'-------'-----'

        TKey fixed binary layout
        .-------------------------.      -.
        | value                   | col_1 |
        .-------------------------.       |
        |       .    .    .       |       | for each schema column, non-null
        .-------------------------.       |
        | value                   | col_K |
        '-------------------------'      -'

        TBtreeIndexNode page binary layout
        - TLabel - page label
        - THeader - page header
        - TPgSize[N] - keys offsets
        - TKey[N] - keys data                <-- var-size
        - TChild[N+1] - children

        TBtreeIndexNode fixed page binary layout
        - TLabel - page label
        - THeader - page header
        - TKey[N] - keys data                <-- fixed-size
        - TChild[N+1] - children
    */

    class TBtreeIndexNode {
    public:
        using TColumns = TArrayRef<const TPartScheme::TColumn>;
        using TCells = TArrayRef<const TCell>;

#pragma pack(push,1)
        struct THeader {
            TRecIdx KeysCount;
            TPgSize KeysSize;
            ui8 FixedKeySize;
        } Y_PACKED;

        static_assert(sizeof(THeader) == 9, "Invalid TBtreeIndexNode THeader size");

        struct TIsNullBitmap {
            static TPos Length(TPos capacity) {
                // round up (capacity / 8)
                return (capacity + 7) >> 3;
            }

            bool IsNull(TPos pos) const {
                ui8 x = reinterpret_cast<const ui8*>(this)[pos >> 3];
                return (x >> (pos & 7)) & 1;
            }

            void SetNull(TPos pos) {
                ui8& x = reinterpret_cast<ui8*>(this)[pos >> 3];
                x |= (1 << (pos & 7));
            }

            // 1 bit = null
            ui8 IsNullBitmap_;
        } Y_PACKED;

        static_assert(sizeof(TIsNullBitmap) == 1, "Invalid TBtreeIndexNode TIsNullBitmap size");

        struct TChild {
            TPageId PageId;
            TRowId Count;
            TRowId ErasedCount;
            ui64 DataSize;

            auto operator<=>(const TChild&) const = default;

            TString ToString() const noexcept
            {
                // copy values to prevent 'reference binding to misaligned address' error
                return TStringBuilder() << "PageId: " << TPageId(PageId) << " Count: " << TRowId(Count) << " Erased: " << TRowId(ErasedCount) << " DataSize: " << ui64(DataSize);
            }
        } Y_PACKED;

        static_assert(sizeof(TChild) == 28, "Invalid TBtreeIndexNode TChild size");

#pragma pack(pop)

        struct TCellsIter {
            TCellsIter(const char* ptr, TColumns columns)
                : IsNullBitmap(nullptr)
                , Columns(columns)
                , Ptr(ptr)
            {
            }

            TCellsIter(const TIsNullBitmap* isNullBitmap, TColumns columns)
                : IsNullBitmap(isNullBitmap)
                , Columns(columns)
                , Ptr(reinterpret_cast<const char*>(IsNullBitmap) + IsNullBitmap->Length(columns.size()))
            {
            }

            TCellsIter(const TIsNullBitmap* isNullBitmap, TColumns columns, const char* ptr)
                : IsNullBitmap(isNullBitmap)
                , Columns(columns)
                , Ptr(ptr)
            {
            }

            TPos Count() const noexcept
            {
                return Columns.size();
            }

            TCell Next()
            {
                Y_ABORT_UNLESS(Pos < Columns.size());

                if (IsNullBitmap && IsNullBitmap->IsNull(Pos)) {
                    Pos++;
                    return { };
                }

                TCell result;

                if (Columns[Pos].IsFixed) {
                    result = TCell(Ptr, Columns[Pos].FixedSize);
                } else {
                    Y_ABORT_UNLESS(IsNullBitmap, "Can't have references in fixed format");
                    auto *ref = TDeref<TDataRef>::At(Ptr);
                    result = TCell(TDeref<const char>::At(Ptr, ref->Offset), ref->Size);
                }
                Ptr += Columns[Pos].FixedSize; // fixed data or data ref size
                Pos++;

                return result;
            }

            int CompareTo(const TCells key, const TKeyCellDefaults *keyDefaults) noexcept
            {
                Y_ABORT_UNLESS(Pos == 0, "Shouldn't be used");

                for (TPos it = 0; it < Min(key.size(), keyDefaults->Size()); it++) {
                    const TCell left = it < Columns.size() ? Next() : keyDefaults->Defs[it];

                    if (int cmp = CompareTypedCells(left, key[it], keyDefaults->Types[it]))
                        return cmp;
                }

                return key.size() < keyDefaults->Size() ? -1 : 0;
            }

        private:
            const TIsNullBitmap* const IsNullBitmap;
            const TColumns Columns;
            const char* Ptr;
            TPos Pos = 0;
        };

        struct TCellsIterable {
            TCellsIterable(const char* ptr, TColumns columns)
                : IsNullBitmap(nullptr)
                , Columns(columns)
                , Ptr(ptr)
            {
            }

            TCellsIterable(const TIsNullBitmap* isNullBitmap, TColumns columns)
                : IsNullBitmap(isNullBitmap)
                , Columns(columns)
                , Ptr(reinterpret_cast<const char*>(IsNullBitmap) + IsNullBitmap->Length(columns.size()))
            {
            }

            TPos Count() const noexcept
            {
                return Columns.size();
            }

            TCellsIter Iter() const noexcept
            {
                return {IsNullBitmap, Columns, Ptr};
            }

            int CompareTo(const TCells key, const TKeyCellDefaults *keyDefaults) const noexcept
            {
                return Iter().CompareTo(key, keyDefaults);
            }

        private:
            const TIsNullBitmap* const IsNullBitmap;
            const TColumns Columns;
            const char* Ptr;
        };

    public:
        TBtreeIndexNode(TSharedData raw)
            : Raw(std::move(raw))
        {
            const auto data = NPage::TLabelWrapper().Read(Raw, EPage::BTreeIndex);
            Y_ABORT_UNLESS(data == ECodec::Plain && data.Version == 0);

            Header = TDeref<const THeader>::At(data.Page.data());
            size_t offset = sizeof(THeader);

            if (IsFixedFormat()) {
                Y_ABORT_UNLESS(Header->KeysSize == static_cast<TPgSize>(Header->KeysCount) * Header->FixedKeySize);
                Keys = TDeref<const TRecordsEntry>::At(Header, offset);
            } else {
                Keys = Raw.data();
                Offsets = TDeref<const TRecordsEntry>::At(Header, offset);
                offset += Header->KeysCount * sizeof(TRecordsEntry);
            }
            offset += Header->KeysSize;

            Children = TDeref<const TChild>::At(Header, offset);
            offset += (1 + Header->KeysCount) * sizeof(TChild);

            Y_ABORT_UNLESS(offset == data.Page.size());
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Raw.data());
        }

        bool IsFixedFormat() const noexcept
        {
            return Header->FixedKeySize != Max<ui8>();
        }

        TRecIdx GetKeysCount() const noexcept
        {
            return Header->KeysCount;
        }

        TRecIdx GetChildrenCount() const noexcept
        {
            return GetKeysCount() + 1;
        }

        TCellsIterable GetKeyCellsIterable(TRecIdx pos, TColumns columns) const noexcept
        {
            return GetCells<TCellsIterable>(pos, columns);
        }

        TCellsIter GetKeyCellsIter(TRecIdx pos, TColumns columns) const noexcept
        {
            return GetCells<TCellsIter>(pos, columns);
        }

        const TChild& GetChild(TRecIdx pos) const noexcept
        {
            return Children[pos];
        }

        static bool Has(TRowId rowId, TRowId beginRowId, TRowId endRowId) noexcept {
            return beginRowId <= rowId && rowId < endRowId;
        }

        TRecIdx Seek(TRowId rowId, std::optional<TRecIdx> on) const noexcept
        {
            const TRecIdx childrenCount = GetChildrenCount();
            if (on && on >= childrenCount) {
                Y_DEBUG_ABORT_UNLESS(false, "Should point to some child");
                on = { };
            }

            const auto cmp = [](TRowId rowId, const TChild& child) {
                return rowId < child.Count;
            };

            TRecIdx result;
            if (!on) {
                // Use a full binary search
                result = std::upper_bound(Children, Children + childrenCount, rowId, cmp) - Children;
            } else if (Children[*on].Count <= rowId) {
                // Try a short linear search first
                result = *on;
                for (int linear = 0; linear < 4; ++linear) {
                    result++;
                    Y_ABORT_UNLESS(result < childrenCount, "Should always seek some child");
                    if (Children[result].Count > rowId) {
                        return result;
                    }
                }

                // Binary search from the next record
                result = std::upper_bound(Children + result + 1, Children + childrenCount, rowId, cmp) - Children;
            } else { // Children[*on].Count > rowId
                // Try a short linear search first
                result = *on;
                for (int linear = 0; linear < 4; ++linear) {
                    if (result == 0) {
                        return 0;
                    }
                    if (Children[result - 1].Count <= rowId) {
                        return result;
                    }
                    result--;
                }

                // Binary search up to current record
                result = std::upper_bound(Children, Children + result, rowId, cmp) - Children;
            }

            Y_ABORT_UNLESS(result < childrenCount, "Should always seek some child");
            return result;
        }

        static bool Has(ESeek seek, TCells key, TCellsIterable beginKey, TCellsIterable endKey, const TKeyCellDefaults *keyDefaults) noexcept
        {
            Y_ABORT_UNLESS(key);
            Y_UNUSED(seek);

            return (!beginKey.Count() || beginKey.CompareTo(key, keyDefaults) <= 0)
                && (!endKey.Count() || endKey.CompareTo(key, keyDefaults) > 0);
        }

        /**
        * Searches for the first child that may contain given key with specified seek mode
        *
        * Result is approximate and may be off by one page
        */
        TRecIdx Seek(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            Y_ABORT_UNLESS(key);
            Y_UNUSED(seek);

            const auto range = xrange(0u, GetKeysCount());

            const auto cmp = [this, columns, keyDefaults](TCells key, TPos pos) {
                return GetKeyCellsIter(pos, columns).CompareTo(key, keyDefaults) > 0;
            };

            // find a first key greater than the given key and then go to its left child
            // if such a key doesn't exist, go to the last child
            return *std::upper_bound(range.begin(), range.end(), key, cmp);
        }

        static bool HasReverse(ESeek seek, TCells key, TCellsIterable beginKey, TCellsIterable endKey, const TKeyCellDefaults *keyDefaults) noexcept
        {
            Y_ABORT_UNLESS(key);

            // ESeek::Upper can skip a page with given key
            const bool endKeyExclusive = seek != ESeek::Upper;
            return (!beginKey.Count() || beginKey.CompareTo(key, keyDefaults) <= 0)
                && (!endKey.Count() || endKey.CompareTo(key, keyDefaults) >= endKeyExclusive);
        }

        /**
        * Searches for the first child (in reverse) that may contain given key with specified seek mode
        *
        * Result is approximate and may be off by one page
        */
        TRecIdx SeekReverse(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            Y_ABORT_UNLESS(key);

            const auto range = xrange(0u, GetKeysCount());

            switch (seek) {
                case ESeek::Exact:
                case ESeek::Lower: {
                    const auto cmp = [this, columns, keyDefaults](TCells key, TPos pos) {
                        return GetKeyCellsIter(pos, columns).CompareTo(key, keyDefaults) > 0;
                    };
                    // find a first key greater than the given key and then go to its left child
                    // if such a key doesn't exist, go to the last child
                    return *std::upper_bound(range.begin(), range.end(), key, cmp);
                };
                case ESeek::Upper: {
                    const auto cmp = [this, columns, keyDefaults](TPos pos, TCells key) {
                        return GetKeyCellsIter(pos, columns).CompareTo(key, keyDefaults) < 0;
                    };
                    // find a first key equal or greater than the given key and then go to its left child
                    // if such a key doesn't exist, go to the last child
                    return *std::lower_bound(range.begin(), range.end(), key, cmp);
                };
                default:
                    Y_ABORT("Unsupported seek mode");
            }
        }

    private:
        template <typename TCellsType>
        TCellsType GetCells(TRecIdx pos, TColumns columns) const noexcept
        {
            if (IsFixedFormat()) {
                const char* ptr = TDeref<const char>::At(Keys, pos * Header->FixedKeySize);
                return TCellsType(ptr, columns);
            } else {
                const TIsNullBitmap* isNullBitmap = TDeref<const TIsNullBitmap>::At(Keys, Offsets[pos].Offset);
                return TCellsType(isNullBitmap, columns);
            }
        }

    private:
        TSharedData Raw;
        const THeader* Header = nullptr;
        const void* Keys = nullptr;
        const TRecordsEntry* Offsets = nullptr;
        const TChild* Children = nullptr;
    };

    struct TBtreeIndexMeta : public TBtreeIndexNode::TChild {
        size_t LevelsCount;
        ui64 IndexSize;

        auto operator<=>(const TBtreeIndexMeta&) const = default;

        TString ToString() const noexcept
        {
            return TStringBuilder() << TBtreeIndexNode::TChild::ToString() << " LevelsCount: " << LevelsCount << " IndexSize: " << IndexSize;
        }
    };
}
