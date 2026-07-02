#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <util/generic/bitmap.h>
#include "flat_page_base.h"
#include "flat_page_label.h"
#include "util_fmt_abort.h"

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

            auto operator<=>(const TShortChild&) const = default;

            inline TPageId GetPageId() const noexcept {
                return PageId_;
            }

            inline TRowId GetRowCount() const noexcept {
                return RowCount_;
            }

            inline ui64 GetDataSize() const noexcept {
                return DataSize_;
            }

            TString ToString() const {
                return TStringBuilder() << "PageId: " << GetPageId() << " RowCount: " << GetRowCount() << " DataSize: " << GetDataSize();
            }
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

            TString ToString() const {
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

        struct TShortChildV2 {
            TPageOffset Offset_; // 8 bytes — byte offset or page index
            ui64 Size_;          // 8 bytes — page body size
            ui32 Crc32_;         // 4 bytes
            TRowId RowCount_;    // 8 bytes  (cumulative)
            ui64 DataSize_;      // 8 bytes  (cumulative)

            auto operator<=>(const TShortChildV2&) const = default;

            inline TPageOffset GetOffset() const noexcept {
                return Offset_;
            }

            inline ui64 GetSize() const noexcept {
                return Size_;
            }

            inline ui32 GetCrc32() const noexcept {
                return Crc32_;
            }

            inline TRowId GetRowCount() const noexcept {
                return RowCount_;
            }

            inline ui64 GetDataSize() const noexcept {
                return DataSize_;
            }

            TPageLocation GetLocation(EPage type) const noexcept {
                return {Offset_, Size_, type, Crc32_};
            }
        } Y_PACKED;

        static_assert(sizeof(TShortChildV2) == 36, "Invalid TBtreeIndexNode TShortChildV2 size");

        struct TChildV2 {
            TPageOffset Offset_;    // 8 bytes — byte offset or page index
            ui64 Size_;             // 8 bytes — page body size
            ui32 Crc32_;            // 4 bytes
            TRowId RowCount_;       // 8 bytes  (cumulative)
            ui64 DataSize_;         // 8 bytes  (cumulative)
            ui64 GroupDataSize_;    // 8 bytes  (cumulative)
            TRowId ErasedRowCount_; // 8 bytes  (cumulative)

            auto operator<=>(const TChildV2&) const = default;

            inline TPageOffset GetOffset() const noexcept {
                return Offset_;
            }

            inline ui64 GetSize() const noexcept {
                return Size_;
            }

            inline ui32 GetCrc32() const noexcept {
                return Crc32_;
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

            TPageLocation GetLocation(EPage type) const noexcept {
                return {Offset_, Size_, type, Crc32_};
            }
        } Y_PACKED;

        static_assert(sizeof(TChildV2) == 52, "Invalid TBtreeIndexNode TChildV2 size");

        // Common-field offset compatibility between long and short v2 children
        static_assert(offsetof(TChildV2, Offset_) == offsetof(TShortChildV2, Offset_));
        static_assert(offsetof(TChildV2, Size_) == offsetof(TShortChildV2, Size_));
        static_assert(offsetof(TChildV2, Crc32_) == offsetof(TShortChildV2, Crc32_));
        static_assert(offsetof(TChildV2, RowCount_) == offsetof(TShortChildV2, RowCount_));
        static_assert(offsetof(TChildV2, DataSize_) == offsetof(TShortChildV2, DataSize_));

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
                Y_ENSURE(Pos < Columns.size());

                if (IsNullBitmap && IsNullBitmap->IsNull(Pos)) {
                    Pos++;
                    return { };
                }

                TCell result;

                if (Columns[Pos].IsFixed) {
                    result = TCell(Ptr, Columns[Pos].FixedSize);
                } else {
                    Y_ENSURE(IsNullBitmap, "Can't have references in fixed format");
                    auto *ref = TDeref<TDataRef>::At(Ptr);
                    result = TCell(TDeref<const char>::At(Ptr, ref->Offset), ref->Size);
                }
                Ptr += Columns[Pos].FixedSize; // fixed data or data ref size
                Pos++;

                return result;
            }

            TCell At(TPos index)
            {
                Y_ENSURE(Pos == 0, "Shouldn't be used");

                // We may use offset = Columns[index].Offset - index * sizeof(TIndex::TItem) for fixed format
                // But it looks too complicated because this method will be used only for history columns 0, 1, 2
                Y_DEBUG_ABORT_UNLESS(index < 3);

                for (TPos i = 0; i < index; i++) {
                    Next();
                }
                return Next();
            }

            int CompareTo(const TCells key, const TKeyCellDefaults *keyDefaults)
            {
                Y_ENSURE(Pos == 0, "Shouldn't be used");

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

            int CompareTo(const TCells key, const TKeyCellDefaults *keyDefaults) const
            {
                return Iter().CompareTo(key, keyDefaults);
            }

            explicit operator bool() const noexcept
            {
                return Count() > 0;
            }

            void Describe(IOutputStream& out, const TKeyCellDefaults& keyDefaults) const
            {
                out << '{';

                auto iter = Iter();
                for (TPos pos : xrange(iter.Count())) {
                    if (pos != 0) {
                        out << ", ";
                    }
                    TString value;
                    DbgPrintValue(value, iter.Next(), keyDefaults.BasicTypes()[pos]);
                    out << value;
                }

                out << '}';
            }

        private:
            const TIsNullBitmap* IsNullBitmap;
            TColumns Columns;
            const char* Ptr;
        };

    public:
        // Version = 0 didn't have GroupDataSize field
        static const ui16 FormatVersion = 1;
        static const ui16 FormatVersionV2 = 2;

        TBtreeIndexNode(TSharedData raw)
            : Raw(std::move(raw))
        {
            const auto data = NPage::TLabelWrapper().Read(Raw, EPage::BTreeIndex);

            Y_ENSURE(data == ECodec::Plain);
            Y_ENSURE(data.Version == FormatVersion || data.Version == FormatVersionV2,
                "Unexpected btree index version " << data.Version);
            StoredVersion = data.Version;

            Header = TDeref<const THeader>::At(data.Page.data());
            size_t offset = sizeof(THeader);

            if (IsFixedFormat()) {
                Y_ENSURE(Header->KeysSize == static_cast<TPgSize>(Header->KeysCount) * Header->FixedKeySize);
                Keys = TDeref<const TRecordsEntry>::At(Header, offset);
            } else {
                Keys = Raw.data();
                Offsets = TDeref<const TRecordsEntry>::At(Header, offset);
                offset += Header->KeysCount * sizeof(TRecordsEntry);
            }
            offset += Header->KeysSize;

            Children = TDeref<const char>::At(Header, offset);
            offset += (1 + Header->KeysCount) * ChildStructSize();

            Y_ENSURE(offset == data.Page.size());
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

        ui16 GetStoredVersion() const noexcept
        {
            return StoredVersion;
        }

        size_t ChildStructSize() const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return Header->IsShortChildFormat ? sizeof(TShortChildV2) : sizeof(TChildV2);
            }
            return Header->IsShortChildFormat ? sizeof(TShortChild) : sizeof(TChild);
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
            return TDeref<const TShortChild>::At(Children, pos * ChildStructSize());
        }

        const TShortChild& GetShortChild(TRecIdx pos) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersion, "GetShortChild on v2 node, use GetShortChildV2");
            return *GetShortChildRef(pos);
        }

        const TChild* GetChildRef(TRecIdx pos) const
        {
            Y_DEBUG_ABORT_UNLESS(!Header->IsShortChildFormat, "GetShortChildRef should be used instead");
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersion, "GetChildRef on v2 node, use GetChildV2");
            return TDeref<const TChild>::At(Children, pos * ChildStructSize());
        }

        const TChild& GetChild(TRecIdx pos) const
        {
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersion, "GetChild on v2 node, use GetChildV2");
            return *GetChildRef(pos);
        }

        const TChildV2& GetChildV2(TRecIdx pos) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersionV2, "GetChildV2 on v1 node");
            return *reinterpret_cast<const TChildV2*>(Children + pos * ChildStructSize());
        }

        const TShortChildV2& GetShortChildV2(TRecIdx pos) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersionV2, "GetShortChildV2 on v1 node");
            return *reinterpret_cast<const TShortChildV2*>(Children + pos * ChildStructSize());
        }

        TRowId GetChildRowCount(TRecIdx pos) const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return GetShortChildV2(pos).GetRowCount();
            }
            return GetShortChild(pos).GetRowCount();
        }

        /// For v1 nodes, the caller must resolve via Part->GetPageLocation(pageId, groupId).
        TPageId GetChildPageId(TRecIdx pos) const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return Max<TPageId>();
            }
            return IsShortChildFormat() ? GetShortChild(pos).GetPageId()
                                        : GetChild(pos).GetPageId();
        }

        /// Returns the child's inline TPageLocation (v2 only).
        TPageLocation GetChildLocationV2(TRecIdx pos, EPage type) const noexcept
        {
            Y_DEBUG_ABORT_UNLESS(StoredVersion == FormatVersionV2, "GetChildLocationV2 on v1 node");
            return IsShortChildFormat() ? GetShortChildV2(pos).GetLocation(type)
                                        : GetChildV2(pos).GetLocation(type);
        }

        /// Version-aware data size access for short children.
        ui64 GetChildDataSize(TRecIdx pos) const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return IsShortChildFormat() ? GetShortChildV2(pos).GetDataSize()
                                            : GetChildV2(pos).GetDataSize();
            }
            return IsShortChildFormat() ? GetShortChild(pos).GetDataSize()
                                        : GetChild(pos).GetDataSize();
        }

        /// Version-aware cumulative row count for the child at pos.
        TRowId GetChildNonErasedRowCount(TRecIdx pos) const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return IsShortChildFormat() ? GetShortChildV2(pos).GetRowCount()
                                            : GetChildV2(pos).GetNonErasedRowCount();
            }
            return IsShortChildFormat() ? GetShortChild(pos).GetRowCount()
                                        : GetChild(pos).GetNonErasedRowCount();
        }

        /// Version-aware previous child cumulative data size.
        ui64 GetPrevChildDataSize(TRecIdx pos) const noexcept
        {
            if (pos == 0) return 0;
            if (StoredVersion == FormatVersionV2) {
                return IsShortChildFormat() ? GetShortChildV2(pos - 1).GetDataSize()
                                            : GetChildV2(pos - 1).GetDataSize();
            }
            return IsShortChildFormat() ? GetShortChild(pos - 1).GetDataSize()
                                        : GetChild(pos - 1).GetDataSize();
        }

        /// Version-aware previous child non-erased row count.
        TRowId GetPrevChildNonErasedRowCount(TRecIdx pos) const noexcept
        {
            if (pos == 0) return 0;
            if (StoredVersion == FormatVersionV2) {
                return IsShortChildFormat() ? GetShortChildV2(pos - 1).GetRowCount()
                                            : GetChildV2(pos - 1).GetNonErasedRowCount();
            }
            return IsShortChildFormat() ? GetShortChild(pos - 1).GetRowCount()
                                        : GetChild(pos - 1).GetNonErasedRowCount();
        }

        ui64 GetChildTotalDataSize(TRecIdx pos) const noexcept
        {
            if (StoredVersion == FormatVersionV2) {
                return IsShortChildFormat() ? GetShortChildV2(pos).GetDataSize()
                                            : GetChildV2(pos).GetTotalDataSize();
            }
            return IsShortChildFormat() ? GetShortChild(pos).GetDataSize()
                                        : GetChild(pos).GetTotalDataSize();
        }

        static bool Has(TRowId rowId, TRowId beginRowId, TRowId endRowId) noexcept {
            return beginRowId <= rowId && rowId < endRowId;
        }

        TRecIdx Seek(TRowId rowId, std::optional<TRecIdx> on = { }) const
        {
            const TRecIdx childrenCount = GetChildrenCount();
            if (on && on >= childrenCount) {
                Y_DEBUG_ABORT("Should point to some child");
                on = { };
            }

            auto range = xrange(0u, childrenCount);
            const auto cmp = [this](TRowId rowId, TPos pos) {
                return rowId < GetChildRowCount(pos);
            };

            TRecIdx result;
            if (!on) {
                // Will do a full binary search on full range
            } else if (GetChildRowCount(*on) <= rowId) {
                // Try a short linear search first
                result = *on;
                for (int linear = 0; linear < 4; ++linear) {
                    result++;
                    Y_ENSURE(result < childrenCount, "Should always seek some child");
                    if (GetChildRowCount(result) > rowId) {
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
                    if (GetChildRowCount(result - 1) <= rowId) {
                        return result;
                    }
                    result--;
                }

                // Will do a binary search up to current record
                range = xrange(0u, result);
            }

            result = *std::upper_bound(range.begin(), range.end(), rowId, cmp);

            Y_ENSURE(result < childrenCount, "Should always seek some child");
            return result;
        }

        static bool Has(ESeek seek, TCells key, TCellsIterable beginKey, TCellsIterable endKey, const TKeyCellDefaults *keyDefaults)
        {
            Y_ENSURE(key);
            Y_UNUSED(seek);

            return (!beginKey || beginKey.CompareTo(key, keyDefaults) <= 0)
                && (!endKey || endKey.CompareTo(key, keyDefaults) > 0);
        }

        /**
        * Searches for the first child that may contain given key with specified seek mode
        *
        * Result is approximate and may be off by one page
        */
        TRecIdx Seek(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults) const
        {
            Y_ENSURE(key);
            Y_UNUSED(seek);

            const auto range = xrange(0u, GetKeysCount());

            const auto cmp = [this, columns, keyDefaults](TCells key, TPos pos) {
                return GetKeyCellsIter(pos, columns).CompareTo(key, keyDefaults) > 0;
            };

            // find a first key greater than the given key and then go to its left child
            // if such a key doesn't exist, go to the last child
            return *std::upper_bound(range.begin(), range.end(), key, cmp);
        }

        static bool HasReverse(ESeek seek, TCells key, TCellsIterable beginKey, TCellsIterable endKey, const TKeyCellDefaults *keyDefaults)
        {
            Y_ENSURE(key);

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
        TRecIdx SeekReverse(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults) const
        {
            Y_ENSURE(key);

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
                    Y_TABLET_ERROR("Unsupported seek mode");
            }
        }

    private:
        template <typename TCellsType>
        TCellsType GetCells(TRecIdx pos, TColumns columns) const
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
        const char* Children = nullptr;
        ui16 StoredVersion = FormatVersion;
    };

    struct TBtreeIndexMeta {
        TPageId V1Root = Max<TPageId>();
        TPageLocation V2Root;
        TRowId RowCount_;
        ui64 DataSize_;
        ui64 GroupDataSize_;
        TRowId ErasedRowCount_;
        ui32 LevelCount;
        ui64 IndexSize;

        auto operator<=>(const TBtreeIndexMeta&) const = default;

        /// Root location for v2 format — root page carries full byte-offset location.
        static TPageLocation RootLocationV2(ui64 offset, ui64 size, ui32 crc32, EPage type) noexcept {
            return TPageLocation::FromByteOffset(offset, size, type, crc32);
        }

        /// Returns true when the v1 root (page-id reference) is available.
        bool HasV1Root() const noexcept
        {
            return V1Root != Max<TPageId>();
        }

        /// Returns true when the v2 root (byte-offset location) is available.
        bool HasV2Root() const noexcept
        {
            return V2Root && V2Root.Offset.IsByteOffset();
        }

        /// Returns the root PageId for v1 format, or Max<TPageId>() when v1 root is not available.
        TPageId RootPageIdV1() const noexcept
        {
            return V1Root;
        }

        inline TRowId GetRowCount() const noexcept { return RowCount_; }
        inline ui64 GetDataSize() const noexcept { return DataSize_; }
        inline ui64 GetGroupDataSize() const noexcept { return GroupDataSize_; }
        inline TRowId GetErasedRowCount() const noexcept { return ErasedRowCount_; }
        inline TRowId GetNonErasedRowCount() const noexcept { return RowCount_ - ErasedRowCount_; }
        inline ui64 GetTotalDataSize() const noexcept { return DataSize_ + GroupDataSize_; }

        /// Build a TChild from the row aggregates (v1 compat).
        TBtreeIndexNode::TChild ToChild() const noexcept {
            return {RootPageIdV1(), GetRowCount(), GetDataSize(), GetGroupDataSize(), GetErasedRowCount()};
        }

        TString ToString() const
        {
            TStringBuilder result;
            result << "V1Root: " << RootPageIdV1() << " V2Root: " << V2Root
                << " RowCount: " << GetRowCount() << " DataSize: " << GetDataSize();
            if (GetGroupDataSize()) {
                result << " GroupDataSize: " << GetGroupDataSize();
            }
            result << " ErasedRowCount: " << GetErasedRowCount()
                << " LevelCount: " << LevelCount << " IndexSize: " << IndexSize;
            return result;
        }
    };
}
