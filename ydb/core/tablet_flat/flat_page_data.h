#pragma once

#include "flat_page_base.h"
#include "flat_page_label.h"
#include "flat_row_nulls.h"
#include "util_basics.h"
#include "util_deref.h"

#include <ydb/library/actors/util/shared_data.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <util/generic/buffer.h>

namespace NKikimr {
namespace NTable {
namespace NPage {


    struct TDataPage {

        /*
                TRecord binary layout v1
            .-------------------------.
            | ERowOp and bit-flags    | header
            .---------.---------------.       -.
            | cell op | value OR offs | cell_1 |
            .---------.---------------.        |
            |       .    .    .       |        | fixed-size
            .---------.---------------.        |
            | cell op | value OR offs | cell_K |
            .-.------.'-.-------.-----.       -'
            | |      |  |       |     | var-size values
            '-'------'--'-------'-----'
        */

#pragma pack(push,1)

        struct TItem {
            TCellOp GetCellOp(bool key) const noexcept
            {
                if (key)
                    return Null ? ECellOp::Null : ECellOp::Set;
                return TCellOp::Decode(Flg);
            }

            union {
                bool Null;
                ui8 Flg;
            };
        } Y_PACKED;

        struct TVersion {
            ui64 Step_;
            ui64 TxId_;

            TRowVersion Get() const noexcept {
                return TRowVersion(Step_, TxId_);
            }

            void Set(const TRowVersion& v) noexcept {
                Step_ = v.Step;
                TxId_ = v.TxId;
            }
        } Y_PACKED;

        struct TDelta {
            ui64 TxId_;

            ui64 GetTxId() const noexcept {
                return TxId_;
            }

            void SetTxId(ui64 txId) noexcept {
                TxId_ = txId;
            }
        } Y_PACKED;

        struct TRecord : public TDataPageRecord<TRecord, TItem> {
            ui8 Fields_;

            ERowOp GetRop() const noexcept {
                return ERowOp(Fields_ & 0x0F);
            }

            bool IsErased() const noexcept {
                return Fields_ & 0x80;
            }

            bool IsVersioned() const noexcept {
                return Fields_ & 0x40;
            }

            bool HasHistory() const noexcept {
                return Fields_ & 0x20;
            }

            bool IsDelta() const noexcept {
                /* HasHistory without other bits set */
                return (Fields_ & 0xF0) == 0x20;
            }

            void SetZero() noexcept {
                Fields_ = 0;
            }

            void SetFields(ERowOp rop, bool erased, bool versioned, bool delta) noexcept {
                Fields_ = ui8(rop) | (delta ? 0x20 : ((erased ? 0x80 : 0) | (versioned ? 0x40 : 0)));
            }

            void MarkHasHistory() noexcept {
                Fields_ |= 0x20;
            }

            // N.B. Get Min/Max version and GetDeltaTxId below read from the same record position
            // right after column entries. Up to caller to decide what kind of data is there

            TRowVersion GetMaxVersion(const TPartScheme::TGroupInfo& group) const noexcept {
                Y_DEBUG_ABORT_UNLESS(IsErased());
                return GetTail<TVersion>(group)->Get();
            }

            TRowVersion GetMinVersion(const TPartScheme::TGroupInfo& group) const noexcept {
                Y_DEBUG_ABORT_UNLESS(IsVersioned());
                auto* v = GetTail<TVersion>(group);
                if (IsErased()) {
                    ++v;
                }
                return v->Get();
            }

            ui64 GetDeltaTxId(const TPartScheme::TGroupInfo& group) const noexcept {
                Y_DEBUG_ABORT_UNLESS(IsDelta());
                return GetTail<TDelta>(group)->GetTxId();
            }

            const TRecord* GetAltRecord(size_t index) const noexcept {
                if (index == 0) {
                    return this;
                }
                const char* base = reinterpret_cast<const char*>(this);
                ui64 offset = ReadUnaligned<ui64>(base - sizeof(ui64) * index);
                if (offset != 0) {
                    return reinterpret_cast<const TRecord*>(base + offset);
                } else {
                    return nullptr;
                }
            }

        private:
            // returns pointer to the data right after all column entries
            template <class T>
            const T* GetTail(const TPartScheme::TGroupInfo& group) const {
                return TDeref<T>::At(Base(), group.FixedSize);
            }
        } Y_PACKED;

        struct TExtra {
            TRowId BaseRow;
        } Y_PACKED;

#pragma pack(pop)

        static_assert(sizeof(TItem) == 1, "Invalid TDataPage TItem size");
        static_assert(sizeof(TVersion) == 16, "Invalid TDataPage TVersion size");
        static_assert(sizeof(TRecord) == 1, "Invalid TDataPage TRecord size");
        static_assert(sizeof(TExtra) == 8, "Invalid TDataPage page extra chunk");

        using TBlock = TBlockWithRecords<TRecord>;

    public:
        using TIter = TBlock::TIterator;

        TDataPage(const TSharedData *raw = nullptr) noexcept
        {
            Set(raw);
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Decoded.data());
        }

        explicit operator bool() const noexcept
        {
            return bool(Decoded);
        }

        const TBlock* operator->() const noexcept
        {
            return &Page;
        }

        TRowId BaseRow() const noexcept
        {
            return BaseRow_;
        }

        TDataPage& Set(const TSharedData *raw = nullptr) noexcept
        {
            Page = { };

            if (raw) {
                const void* base = raw->data();
                auto data = NPage::TLabelWrapper().Read(*raw, EPage::DataPage);

                Y_ABORT_UNLESS(data.Version == 1, "Unknown EPage::DataPage version");

                if (data.Codec != ECodec::Plain) {
                    /* Compressed, should convert to regular page */

                    Y_ABORT_UNLESS(data == ECodec::LZ4, "Only LZ4 encoding allowed");

                    Codec = Codec ? Codec : NBlockCodecs::Codec("lz4fast");
                    auto size = Codec->DecompressedLength(data.Page);

                    // We expect original page had the same label size as a compressed page
                    size_t labelSize = reinterpret_cast<const char*>(data.Page.data()) - reinterpret_cast<const char*>(base);

                    Decoded = TSharedData::Uninitialized(labelSize + size);

                    size = Codec->Decompress(data.Page, Decoded.mutable_begin() + labelSize);

                    Decoded.TrimBack(labelSize + size);
                    ::memcpy(Decoded.mutable_begin(), base, labelSize);

                    base = Decoded.begin();
                    data.Page = { Decoded.begin() + labelSize, Decoded.end() };
                } else {
                    Decoded = *raw;
                }

                auto *recordsHeader = TDeref<TRecordsHeader>::At(data.Page.data(), 0);
                auto count = recordsHeader->Count;

                BaseRow_ = TDeref<const TExtra>::At(recordsHeader + 1, 0)->BaseRow;
                Page.Base = base;
                auto offsetsOffset = data.Page.size() - count * sizeof(TPgSize);
                Page.Offsets = TDeref<const TRecordsEntry>::At(recordsHeader, offsetsOffset);
                Page.Count = count;
            } else {
                Decoded = {};
                BaseRow_ = Max<TRowId>();
            }

            return *this;
        }

        const TSharedData& GetData() const noexcept {
            return Decoded;
        }

        TIter LookupKey(TCells key, const TPartScheme::TGroupInfo &group,
                        ESeek seek, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            if (!key) {
                switch (seek) {
                    case ESeek::Lower:
                        return Page.Begin();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            TIter it;

            const auto cmp = TCompare<TRecord>(group.ColsKeyData, *keyDefaults);
            switch (seek) {
                case ESeek::Exact:
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    // key <= it->Key
                    if (it && cmp(key, *it)) {
                        it = Page.End();
                    }
                    return it;
                case ESeek::Lower: {
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                }
                case ESeek::Upper: {
                    it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                }
            }

            return it;
        }

        TIter LookupKeyReverse(TCells key, const TPartScheme::TGroupInfo &group,
                        ESeek seek, const TKeyCellDefaults *keyDefaults) const noexcept
        {
            if (!key) {
                switch (seek) {
                    case ESeek::Lower:
                        return --Page.End();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            TIter it;

            const auto cmp = TCompare<TRecord>(group.ColsKeyData, *keyDefaults);
            switch (seek) {
                case ESeek::Exact:
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    // key <= it->Key
                    if (it && cmp(key, *it)) {
                        it = Page.End();
                    }
                    return it;
                case ESeek::Lower:
                    it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                case ESeek::Upper:
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

    private:
        using ICodec = NBlockCodecs::ICodec;

        TSharedData Decoded;
        TBlock Page;
        TRowId BaseRow_ = Max<TRowId>();
        const ICodec *Codec = nullptr;
    };
}
}
}
