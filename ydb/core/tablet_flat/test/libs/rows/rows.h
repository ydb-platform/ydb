#pragma once

#include "misc.h"

#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tablet_flat/flat_row_column.h>
#include <ydb/core/tablet_flat/flat_sausage_solid.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/vector.h>
#include <util/generic/utility.h>

namespace NKikimr {

namespace NScheme {
    template<typename TVal> struct TTypeFor;

    template<> struct TTypeFor<ui64> { enum { Type = NTypeIds::Uint64 }; };
    template<> struct TTypeFor<ui32> { enum { Type = NTypeIds::Uint32 }; };
    template<> struct TTypeFor<i64> { enum { Type = NTypeIds::Int64 }; };
    template<> struct TTypeFor<i32> { enum { Type = NTypeIds::Int32 }; };
    template<> struct TTypeFor<ui8> { enum { Type = NTypeIds::Byte }; };
    template<> struct TTypeFor<bool> { enum { Type = NTypeIds::Bool }; };
    template<> struct TTypeFor<double> { enum { Type = NTypeIds::Double }; };
    template<> struct TTypeFor<float> { enum { Type = NTypeIds::Float }; };
    template<> struct TTypeFor<::TString> { enum { Type = NTypeIds::String }; };
}

namespace NTable {

namespace NTest {
    namespace ETypes = NScheme::NTypeIds;

    template<typename TVal>
    inline TCell Cimple(const TVal &val)
    {
        static_assert(TCell::CanInline(sizeof(val)), "");

        return { (const char*)(&val), sizeof(val) };
    }

    struct TOmit{ };

    class TRow {
    public:
        using TType = NScheme::TTypeId;

        template<typename TVal> using TTypeFor = NScheme::TTypeFor<TVal>;

        struct TUpdate {
            TUpdate(TTag tag, TType type, TCellOp op)
                : Tag(tag), Type(type), Op(op) { }

            TRawTypeValue Raw() const
            {
                return { Cell.Data(), Cell.Size(), Type };
            }

            NTable::TTag Tag = Max<TTag>();
            TType Type = 0;
            TCellOp Op = ECellOp::Set;
            TCell Cell;
        };

        TRow(TIntrusivePtr<TGrowHeap> heap = new TGrowHeap(512))
            : Heap(heap)
        {
            Cols.reserve(8);
        }

        TRow(TRow &&row) noexcept
            : Heap(std::move(row.Heap))
            , Cols(std::move(row.Cols))
        {

        }

        TArrayRef<const TUpdate> operator*() const noexcept
        {
            return Cols;
        }

        template<typename TVal>
        TRow& Do(NTable::TTag tag, const TVal &val)
        {
            return Put(tag, TTypeFor<TVal>::Type, &val, sizeof(val));
        }

        TRow& Do(NTable::TTag, const TOmit&) noexcept
        {
            return *this; /* implicit ECellOp::Empty */
        }

        TRow& Do(NTable::TTag tag, ECellOp op)
        {
            Y_ABORT_UNLESS(TCellOp::HaveNoPayload(op), "Allowed only payloadless ops");

            Cols.emplace_back(tag, 0, op);

            return *this;
        }

        TRow& Do(NTable::TTag tag, std::nullptr_t)
        {
            return Put(tag, 0, nullptr, 0);
        }

        TRow& Do(NTable::TTag tag, const char *data)
        {
            return Put(tag, TTypeFor<TString>::Type, data, std::strlen(data));
        }

        TRow& Do(NTable::TTag tag, TStringBuf buf)
        {
            return Put(tag, TTypeFor<TString>::Type, buf.data(), buf.length());
        }

        TRow& Do(NTable::TTag tag, const TString &buf)
        {
            return Put(tag, TTypeFor<TString>::Type, buf.data(), buf.size());
        }

        TRow& Do(NTable::TTag tag, const TString &buf, TType type)
        {
            return Put(tag, type, buf.data(), buf.size());
        }

        TRow& Do(NTable::TTag tag, const NPageCollection::TGlobId &glob)
        {
            auto *data = static_cast<const void*>(&glob);

            Put(tag, TTypeFor<TString>::Type, data, sizeof(glob));

            Cols.back().Op = TCellOp{ ECellOp::Set, ELargeObj::GlobId };

            return *this;
        }

        TRow& Add(const TRow &tagged)
        {
            for (const auto &va: *tagged) {
                if (va.Op == ECellOp::Set) {
                    Put(va.Tag, va.Type, va.Cell.Data(), va.Cell.Size());
                } else {
                    Cols.emplace_back(va.Tag, 0, va.Op);
                }
            }

            return *this;
        }

        const TUpdate* Get(NTable::TTag tag) const noexcept
        {
            auto pred = [tag] (const TUpdate &up) { return up.Tag == tag; };

            auto it = std::find_if(Cols.begin(), Cols.end(), pred);

            return it == Cols.end() ? nullptr : &*it;
        }

    private:
        TRow& Put(TTag tag, TType type, const void* ptr_, ui32 len)
        {
            auto *ptr = static_cast<const char*>(ptr_);

            Cols.emplace_back(tag, type, ECellOp::Set);

            if (TCell::CanInline(len)) {
                Cols.back().Cell = { ptr, len };

            } else {
                auto *place = Heap->Alloc(len);

                std::copy(ptr, ptr + len, static_cast<char*>(place));

                Cols.back().Cell = { static_cast<const char*>(place), len };
            }

            Y_ABORT_UNLESS(Cols.back().Cell.IsInline() == TCell::CanInline(len));

            return *this;
        }

    private:
        TIntrusivePtr<TGrowHeap> Heap;
        TVector<TUpdate> Cols;
    };

}
}
}
