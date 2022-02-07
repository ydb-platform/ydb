#pragma once

#include "flat_util_misc.h"
#include "util_basics.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/array_ref.h>

namespace NKikimr {
namespace NTable {

    class TNulls: public TAtomicRefCount<TNulls, NUtil::TDtorDel<TNulls>> {
    protected:
        using TType = NScheme::TTypeId;
        using TOrder = NScheme::TTypeIdOrder;

        TNulls(TArrayRef<const TType> types, TArrayRef<const TCell> defs)
            : Types(types)
            , Defs(defs)
        {
            Y_VERIFY(Defs.size() > 0 && Defs.size() == Types.size());
        }

    public:
        virtual ~TNulls() = default;

    protected:
        template<class TSelf>
        static TIntrusiveConstPtr<TSelf> Make(
                TArrayRef<const TType> types,
                TArrayRef<const TOrder> order,
                TArrayRef<const TCell> defs) noexcept
        {
            size_t offT = AlignUp(sizeof(TSelf));
            size_t offO = offT + AlignUp(sizeof(TType) * types.size());
            size_t offC = offO + AlignUp(sizeof(TOrder) * order.size());
            size_t offD = offC + AlignUp(sizeof(TCell) * defs.size());

            size_t tail = std::accumulate(defs.begin(), defs.end(), size_t(0),
                [](size_t sum, const TCell &cell) {
                    auto size = cell.IsInline() ? 0 : cell.Size();

                    return sum + AlignUp(size_t(size));
                });

            char * const raw = (char*)::operator new(offD + tail);

            TType *ptrT = reinterpret_cast<TType*>(raw + offT);
            std::copy(types.begin(), types.end(), ptrT);

            TOrder *ptrO = reinterpret_cast<TOrder*>(raw + offO);
            std::copy(order.begin(), order.end(), ptrO);

            TCell *ptrC = reinterpret_cast<TCell*>(raw + offC);
            char *data = raw + offD;

            for (size_t it = 0; it < defs.size(); it++) {
                if (defs[it].IsInline()) {
                    ptrC[it] = defs[it];
                } else if (auto * const src = defs[it].Data()) {
                    std::copy(src, src + defs[it].Size(), data);

                    ptrC[it] = { data, defs[it].Size() };

                    data += AlignUp(size_t(defs[it].Size()));
                }
            }

            Y_VERIFY(data == raw + offD + tail);

            return ::new(raw) TSelf(
                    { ptrT, types.size() },
                    { ptrO, order.size() },
                    { ptrC, defs.size() });
        }

    public:
        size_t Size() const noexcept
        {
            return Defs.size();
        }

        TArrayRef<const TCell> operator*() const noexcept
        {
            return Defs;
        }

        const TArrayRef<const TCell>* operator->() const noexcept
        {
            return &Defs;
        }

        const TCell& operator[](size_t on) const noexcept
        {
            return Defs[on];
        }

    public:
        const TArrayRef<const TType> Types;
        const TArrayRef<const TCell> Defs;
    };

    /**
     * Types and defaults for the complete row
     */
    class TRowNulls : public TNulls {
        friend TNulls;

        TRowNulls(
                TArrayRef<const TType> types,
                TArrayRef<const TOrder> order,
                TArrayRef<const TCell> defs)
            : TNulls(types, defs)
        {
            Y_VERIFY(order.size() == 0);
        }

    public:
        static TIntrusiveConstPtr<TRowNulls> Make(
                TArrayRef<const TType> types,
                TArrayRef<const TCell> defs) noexcept
        {
            return TNulls::Make<TRowNulls>(types, { }, defs);
        }
    };

    /**
     * Types and defaults for key columns with per-column ordering
     */
    class TKeyNulls : public TNulls {
        friend TNulls;

        TKeyNulls(
                TArrayRef<const TType> types,
                TArrayRef<const TOrder> order,
                TArrayRef<const TCell> defs)
            : TNulls(types, defs)
            , Types(order)
        {
            Y_VERIFY(Types.size() == TNulls::Types.size());
        }

    public:
        static TIntrusiveConstPtr<TKeyNulls> Make(
                TArrayRef<const TOrder> order,
                TArrayRef<const TCell> defs) noexcept
        {
            TStackVec<TType> types;
            types.reserve(order.size());
            for (TOrder typeOrder : order) {
                types.push_back(typeOrder.GetTypeId());
            }
            return TNulls::Make<TKeyNulls>(types, order, defs);
        }

        TArrayRef<const TType> BasicTypes() const noexcept
        {
            return TNulls::Types;
        }

    public:
        // Shadow base types forcing most code to use order information
        const TArrayRef<const TOrder> Types;
    };

}
}
