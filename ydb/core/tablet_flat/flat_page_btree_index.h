#pragma once

#include <ydb/core/base/defs.h>
#include <util/generic/bitmap.h>
#include "flat_page_base.h"
#include "flat_page_label.h"

namespace NKikimr::NTable::NPage {
    
    /*
        TKey binary layout
        .---------.---------------.
        | Non-null cell bitmap    | for all schema key columns
        .---------.---------------.      -.
        | value OR offs           | col_1 |
        .---------.---------------.       |
        |       .    .    .       |       | for each non-null column
        .---------.---------------.       |
        | value OR offs           | col_K |
        .-.------.--.-------.-----.      -'
        | |      |  |       |     | var-size values
        '-'------'--'-------'-----'

        TBtreeIndexNode page binary layout
        - TLabel - page label
        - THeader - page header
        - TKey[N] - keys data                <-- var-size
        - TPgSize[N] - keys offsets
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
        } Y_PACKED;

        static_assert(sizeof(THeader) == 8, "Invalid TBtreeIndexNode THeader size");

        struct TKey {
            struct TCellsIter {
                TCellsIter(const TKey* key, TColumns columns)
                    : Key(key)
                    , Columns(columns)
                    , Pos(0)
                {
                    Ptr = (char*)Key + key->NullBitmapLength(columns.size());
                }

                TPos Count()
                {
                    return Columns.size();
                }

                TCell Next()
                {
                    Y_ABORT_UNLESS(Pos < Columns.size());

                    if (Key->IsNull(Pos)) {
                        Pos++;
                        return { };
                    }

                    TCell result;

                    if (Columns[Pos].IsFixed) {
                        result = TCell(Ptr, Columns[Pos].FixedSize);
                    } else {
                        auto *ref = TDeref<TDataRef>::At(Ptr);
                        result = TCell(TDeref<const char>::At(Ptr, ref->Offset), ref->Size);
                    }
                    Ptr += Columns[Pos].FixedSize; // fixed data or data ref size
                    Pos++;

                    return result;
                }

            private:
                const TKey* const Key;
                const TColumns Columns;
                TPos Pos;
                const char* Ptr;
            };

            static TPos NullBitmapLength(TPos capacity) {
                // round up (capacity / 8)
                return (capacity + 7) >> 3;
            }

            bool IsNull(TPos pos) const {
                ui8 x = IsNullBitmap[pos >> 3];
                return (x >> (pos & 7)) & 1;
            }

            void SetNull(TPos pos) {
                ui8& x = IsNullBitmap[pos >> 3];
                x |= (1 << (pos & 7));
            }

            // 1 = null
            ui8 IsNullBitmap[0];
        } Y_PACKED;

        static_assert(sizeof(TKey) == 0, "Invalid TBtreeIndexNode TKey size");

        struct TChild {
            TPageId PageId;
            TRowId Count;
            TRowId ErasedCount;
            ui64 Size;

            auto operator<=>(const TChild&) const = default;

            TString ToString() const noexcept
            {
                return TStringBuilder() << "PageId: " << PageId << " Count: " << Count << " Size: " << Size;
            }
        } Y_PACKED;

        static_assert(sizeof(TChild) == 28, "Invalid TBtreeIndexNode TChild size");

#pragma pack(pop)

    public:
        TBtreeIndexNode(TSharedData raw)
            : Raw(std::move(raw)) 
        {
            const auto data = NPage::TLabelWrapper().Read(Raw, EPage::BTreeIndex);
            Y_ABORT_UNLESS(data == ECodec::Plain && data.Version == 0);

            auto *header = TDeref<const THeader>::At(data.Page.data());
            Keys.Count = header->KeysCount;
            size_t offset = sizeof(THeader);

            Keys.Base = Raw.data();
            offset += header->KeysSize;

            Keys.Offsets = TDeref<const TRecordsEntry>::At(header, offset);
            offset += Keys.Count * sizeof(TRecordsEntry);

            Children = TDeref<const TChild>::At(header, offset);
            offset += (1 + Keys.Count) * sizeof(TChild);

            Y_ABORT_UNLESS(offset == data.Page.size());
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Raw.data());
        }

        TRecIdx GetKeysCount() const noexcept
        {
            return Keys.Count;
        }

        TKey::TCellsIter GetKeyCells(TRecIdx pos, TColumns columns) const noexcept
        {
            return TKey::TCellsIter(Keys.Record(pos), columns);
        }

        const TChild& GetChild(TPos pos) const noexcept
        {
            return Children[pos];
        }

        // TODO: Seek methods will go here

    private:
        TSharedData Raw;
        TBlockWithRecords<TKey> Keys;
        const TChild* Children;
    };

}
