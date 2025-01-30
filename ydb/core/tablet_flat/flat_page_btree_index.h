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
            ui8 IsShortChildFormat : 1;
            ui8 FixedKeySize : 7;
            
            const static ui8 MaxFixedKeySize = 127;
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

        struct TShortChild {
            TPageId PageId_;
            TRowId RowCount_;
            ui64 DataSize_;

            inline TPageId GetPageId() const noexcept {
                return PageId_;
            }

            inline TRowId GetRowCount() const noexcept {
                return RowCount_;
            }

            inline ui64 GetDataSize() const noexcept {
                return DataSize_;
            }

            auto operator<=>(const TShortChild&) const = default;
        } Y_PACKED;

        static_assert(sizeof(TShortChild) == 20, "Invalid TBtreeIndexNode TShortChild size");

        struct TChild {
            TPageId PageId_;
            TRowId RowCount_;
            ui64 DataSize_;
            ui64 GroupDataSize_;
            TRowId ErasedRowCount_;

            auto operator<=>(const TChild&) const = default;

            inline TPageId GetPageId() const noexcept {
                return PageId_;
            }

            inline TRowId GetRowCount() const noexcept {
                return RowCount_;
            }

            inline ui64 GetDataSize() const noexcept {
                return DataSize_;
            }

            inline ui64 GetGroupDataSize() const noexcept {
                return GroupDataSize_;
            }

            inline TRowId GetErasedRowCount() const noexcept {
                return ErasedRowCount_;
            }

            inline TRowId GetNonErasedRowCount() const noexcept {
                return RowCount_ - ErasedRowCount_;
            }

            inline ui64 GetTotalDataSize() const noexcept {
                return DataSize_ + GroupDataSize_;
            }

            TString ToString() const noexcept {
                TStringBuilder result;
                result << "PageId: " << GetPageId() << " RowCount: " << GetRowCount() << " DataSize: " << GetDataSize();
                if (GetGroupDataSize()) {
                    result << " GroupDataSize: " << GetGroupDataSize();
                }
                result << " ErasedRowCount: " << GetErasedRowCount();
                return result;
            }
        } Y_PACKED;

        static_assert(sizeof(TChild) == 36, "Invalid TBtreeIndexNode TChild size");

        static_assert(offsetof(TChild, PageId_) == offsetof(TShortChild, PageId_));
        static_assert(offsetof(TChild, RowCount_) == offsetof(TShortChild, RowCount_));
        static_assert(offsetof(TChild, DataSize_) == offsetof(TShortChild, DataSize_));

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

            TCell At(TPos index)
            {
                Y_ABORT_UNLESS(Pos == 0, "Shouldn't be used");

                // We may use offset = Columns[index].Offset - index * sizeof(TIndex::TItem) for fixed format
                // But it looks too complicated because this method will be used only for history columns 0, 1, 2
                Y_DEBUG_ABORT_UNLESS(index < 3);

                for (TPos i = 0; i < index; i++) {
                    Next();
                }
                return Next();
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

            explicit operator bool() const noexcept
            {
                return Count() > 0;
            }

        private:
            const TIsNullBitmap* IsNullBitmap;
            TColumns Columns;
            const char* Ptr;
        };

    public:
        // Version = 0 didn't have GroupDataSize field
        static const ui16 FormatVersion = 1;

        TBtreeIndexNode(TSharedData raw)
            : Raw(std::move(raw))
        {
            const auto data = NPage::TLabelWrapper().Read(Raw, EPage::BTreeIndex);

            Y_ABORT_UNLESS(data == ECodec::Plain && data.Version == FormatVersion);

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
            offset += (1 + Header->KeysCount) * (Header->IsShortChildFormat ? sizeof(TShortChild) : sizeof(TChild));

            Y_ABORT_UNLESS(offset == data.Page.size());
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Raw.data());
        }

        bool IsShortChildFormat() const noexcept
        {
            return Header->IsShortChildFormat;
        }

        bool IsFixedFormat() const noexcept
        {
            return Header->FixedKeySize != Header->MaxFixedKeySize;
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

        const TShortChild* GetShortChildRef(TRecIdx pos) const noexcept
        {
            return TDeref<const TShortChild>::At(Children, 
                pos * (Header->IsShortChildFormat ? sizeof(TShortChild) : sizeof(TChild)));
        }

        const TShortChild& GetShortChild(TRecIdx pos) const noexcept
        {
            return *GetShortChildRef(pos);
        }

        const TChild* GetChildRef(TRecIdx pos) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(!Header->IsShortChildFormat, "GetShortChildRef should be used instead");
            return TDeref<const TChild>::At(Children, pos * sizeof(TChild));
        }

        const TChild& GetChild(TRecIdx pos) const noexcept
        {
            return *GetChildRef(pos);
        }

        static bool Has(TRowId rowId, TRowId beginRowId, TRowId endRowId) noexcept {
            return beginRowId <= rowId && rowId < endRowId;
        }

        TRecIdx Seek(TRowId rowId, std::optional<TRecIdx> on = { }) const noexcept
        {
            const TRecIdx childrenCount = GetChildrenCount();
            if (on && on >= childrenCount) {
                Y_DEBUG_ABORT("Should point to some child");
                on = { };
            }

            auto range = xrange(0u, childrenCount);
            const auto cmp = [this](TRowId rowId, TPos pos) {
                return rowId < GetShortChild(pos).GetRowCount();
            };

            TRecIdx result;
            if (!on) {
                // Will do a full binary search on full range
            } else if (GetShortChild(*on).GetRowCount() <= rowId) {
                // Try a short linear search first
                result = *on;
                for (int linear = 0; linear < 4; ++linear) {
                    result++;
                    Y_ABORT_UNLESS(result < childrenCount, "Should always seek some child");
                    if (GetShortChild(result).GetRowCount() > rowId) {
                        return result;
                    }
                }

                // Will do a binary search from the next record
                range = xrange(result + 1, childrenCount);
            } else { // Children[*on].Count > rowId
                // Try a short linear search first
                result = *on;
                for (int linear = 0; linear < 4; ++linear) {
                    if (result == 0) {
                        return 0;
                    }
                    if (GetShortChild(result - 1).GetRowCount() <= rowId) {
                        return result;
                    }
                    result--;
                }

                // Will do a binary search up to current record
                range = xrange(0u, result);
            }

            result = *std::upper_bound(range.begin(), range.end(), rowId, cmp);

            Y_ABORT_UNLESS(result < childrenCount, "Should always seek some child");
            return result;
        }

        static bool Has(ESeek seek, TCells key, TCellsIterable beginKey, TCellsIterable endKey, const TKeyCellDefaults *keyDefaults) noexcept
        {
            Y_ABORT_UNLESS(key);
            Y_UNUSED(seek);

            return (!beginKey || beginKey.CompareTo(key, keyDefaults) <= 0)
                && (!endKey || endKey.CompareTo(key, keyDefaults) > 0);
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
            return (!beginKey || beginKey.CompareTo(key, keyDefaults) <= 0)
                && (!endKey || endKey.CompareTo(key, keyDefaults) >= endKeyExclusive);
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
        const void* Children = nullptr;
    };

    struct TBtreeIndexMeta : public TBtreeIndexNode::TChild {
        ui32 LevelCount;
        ui64 IndexSize;

        auto operator<=>(const TBtreeIndexMeta&) const = default;

        TString ToString() const noexcept
        {
            return TStringBuilder() << TBtreeIndexNode::TChild::ToString() << " LevelCount: " << LevelCount << " IndexSize: " << IndexSize;
        }
    };
}
