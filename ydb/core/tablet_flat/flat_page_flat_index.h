#pragma once

#include "flat_page_base.h"
#include "flat_page_label.h"
#include "flat_row_nulls.h"
#include "util_deref.h"

namespace NKikimr {
namespace NTable {
namespace NPage {

    struct TFlatIndex {
        /*
                TRecord binary layout v2
            .---------.---------------.
            | TRowId  | page id       | header
            .---------.---------------.      -.
            | is_null | value OR offs | key_1 |
            .---------.---------------.       |
            |       .    .    .       |       | fixed-size
            .---------.---------------.       |
            | is_null | value OR offs | key_K |
            .-.------.--.-------.-----.      -'
            | |      |  |       |     | var-size values
            '-'------'--'-------'-----'
        */

#pragma pack(push,1)

        struct TItem {
            TCellOp GetCellOp(bool) const noexcept
            {
                if (Null)
                    return TCellOp(ECellOp::Null);
                return TCellOp(ECellOp::Set, ELargeObj::Inline);
            }

            bool Null;
        } Y_PACKED;

        struct TRecord : public TDataPageRecord<TRecord, TItem> {
            TRowId RowId_;
            TPageId PageId_;

            inline TRowId GetRowId() const { return RowId_; }
            inline TPageId GetPageId() const { return PageId_; }

            inline void SetRowId(TRowId value) { RowId_ = value; }
            inline void SetPageId(TPageId value) { PageId_ = value; }
        } Y_PACKED;

#pragma pack(pop)

        static_assert(sizeof(TItem) == 1, "Invalid TIndex TItem size");
        static_assert(sizeof(TRecord) == 12, "Invalid TIndex TRecord size");

        using TBlock = TBlockWithRecords<TRecord>;

    public:
        using TIter = typename TBlock::TIterator;

        static constexpr ui16 Version = 3;

        TFlatIndex(TSharedData raw)
            : Raw(std::move(raw))
        {
            const auto data = NPage::TLabelWrapper().Read(Raw, EPage::FlatIndex);
            Y_ABORT_UNLESS(data == ECodec::Plain && (data.Version == 2 || data.Version == 3));

            auto *recordsHeader = TDeref<const TRecordsHeader>::At(data.Page.data(), 0);
            auto count = recordsHeader->Count;
            Y_ABORT_UNLESS(count >= 1u + (data.Version == 3 ? 1 : 0));

            Page.Base = Raw.data();
            auto offsetsOffset = data.Page.size() - count * sizeof(TPgSize);
            Page.Offsets = TDeref<const TRecordsEntry>::At(recordsHeader, offsetsOffset);
            Page.Count = count - (data.Version == 3 ? 1 : 0);
            LastKey = (data.Version == 3) ? Page.Record(Page.Count) : nullptr;
            EndRowId = LastKey ? LastKey->GetRowId() + 1 : Max<TRowId>();
        }

        const TBlock* operator->() const noexcept
        {
            return &Page;
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Raw.data());
        }

        /**
         * Lookup a page that contains rowId
         */
        TIter LookupRow(TRowId rowId, TIter on = { }) const
        {
            if (rowId >= EndRowId) {
                return Page.End();
            }

            const auto cmp = [](TRowId rowId, const TRecord& record) {
                return rowId < record.GetRowId();
            };

            if (!on) {
                // Use a full binary search
                on = std::upper_bound(Page.Begin(), Page.End(), rowId, cmp);
            } else if (on->GetRowId() == rowId) {
                return on;
            } else if (on->GetRowId() < rowId) {
                // Try a short linear search first
                for (int linear = 0; linear < 4; ++linear) {
                    auto next = on + 1;
                    if (!next || rowId < next->GetRowId()) {
                        return on;
                    }
                    if (next->GetRowId() == rowId) {
                        return next;
                    }
                    on = next;
                }

                // Binary search from the next record
                on = std::upper_bound(on + 1, Page.End(), rowId, cmp);
            } else {
                // Try a short linear search first
                for (int linear = 0; linear < 4; ++linear) {
                    auto prev = on - 1;
                    Y_DEBUG_ABORT_UNLESS(prev, "Unexpected failure to find an index record");
                    if (prev->GetRowId() <= rowId) {
                        return prev;
                    }
                    on = prev;
                }

                // Binary search up to current record
                on = std::upper_bound(Page.Begin(), on, rowId, cmp);
            }

            --on;
            Y_DEBUG_ABORT_UNLESS(on, "Unexpected failure to find an index record");
            return on;
        }

        /**
         * Lookup a page that may contain key with specified seek mode
         *
         * Returns end iterator when there is definitely no such page,
         * otherwise the result is approximate and may be off by one page.
         */
        TIter LookupKey(
                TCells key, const TPartScheme::TGroupInfo &group,
                const ESeek seek, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            if (!key) {
                // Special treatment for an empty key
                switch (seek) {
                    case ESeek::Lower:
                        return Page.Begin();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            const auto cmp = TCompare<TRecord>(group.ColsKeyIdx, *keyDefaults);

            // N.B. we know that key < it->Key
            TIter it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);

            // If LastKey < key then the needed page doesn't exist
            if (!it && LastKey && cmp(*LastKey, key)) {
                return it;
            }

            if (it.Off() == 0) {
                // If key < FirstKey then exact key doesn't exist
                if (seek == ESeek::Exact) {
                    it = Page.End();
                }
            } else {
                // N.B. we know that prev->Key <= key
                --it;
            }

            return it;
        }

        /**
         * Lookup a page that may contain key with specified seek mode
         *
         * Returns end iterator when there is definitely no such page,
         * otherwise the result is approximate and may be off by one page.
         */
        TIter LookupKeyReverse(
                TCells key, const TPartScheme::TGroupInfo &group,
                const ESeek seek, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            if (!key) {
                // Special treatment for an empty key
                switch (seek) {
                    case ESeek::Lower:
                        return --Page.End();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            const auto cmp = TCompare<TRecord>(group.ColsKeyIdx, *keyDefaults);

            TIter it;
            switch (seek) {
                case ESeek::Exact:
                case ESeek::Lower:
                    // N.B. we know that key < it->Key
                    it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                case ESeek::Upper:
                    // N.B. we know that key <= it->Key
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
            }

            if (it.Off() == 0) {
                it = Page.End();
            } else {
                --it;
            }

            return it;
        }

        /**
         * Returns row id of the last record on a page
         */
        TRowId GetLastRowId(TIter it) const noexcept {
            if (Y_LIKELY(it)) {
                if (++it) {
                    return it->GetRowId() - 1;
                }
            }
            return LastKey ? LastKey->GetRowId() : Max<TRowId>();
        }

        ui64 Rows() const noexcept
        {
            if (LastKey) {
                return LastKey->GetRowId() + 1; /* exact number of rows */
            } else if (auto iter = --Page.End()) {
                auto pages = (iter - Page.Begin()) + 1;

                return iter->GetRowId() * (pages + 1) / pages;
            } else
                return 0; /* cannot estimate rows for one page part */
        }

        const TRecord* GetLastKeyRecord() const noexcept
        {
            return LastKey;
        }

        TRowId GetEndRowId() const noexcept
        {
            return EndRowId;
        }

    private:
        TSharedData Raw;
        TBlock Page;
        const TRecord* LastKey;
        TRowId EndRowId;
    };

}
}
}
