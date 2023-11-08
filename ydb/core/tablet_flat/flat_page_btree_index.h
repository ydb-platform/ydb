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

#pragma pack(push,1)
    public:
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

            TPos Count()
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

        private:
            const TIsNullBitmap* const IsNullBitmap;
            const TColumns Columns;
            const char* Ptr;
            TPos Pos = 0;
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

        TCellsIter GetKeyCells(TRecIdx pos, TColumns columns) const noexcept
        {
            if (IsFixedFormat()) {
                const char* ptr = TDeref<const char>::At(Keys, pos * Header->FixedKeySize);
                return TCellsIter(ptr, columns);
            } else {
                const TIsNullBitmap* isNullBitmap = TDeref<const TIsNullBitmap>::At(Keys, Offsets[pos].Offset);
                return TCellsIter(isNullBitmap, columns);
            }
        }

        const TChild& GetChild(TPos pos) const noexcept
        {
            return Children[pos];
        }

        // TODO: Seek methods will go here

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
