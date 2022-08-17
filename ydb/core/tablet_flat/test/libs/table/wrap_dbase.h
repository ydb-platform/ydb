#pragma once

#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<class TIter>
    struct TWrapDbIterImpl {

        TWrapDbIterImpl(TDatabase &base, ui32 table, TIntrusiveConstPtr<TRowScheme> scheme,
                TRowVersion snapshot = TRowVersion::Max(),
                ENext mode = ENext::All)
            : Scheme(std::move(scheme))
            , Base(base)
            , Table(table)
            , Snapshot(snapshot)
            , Mode(mode)
        {

        }

        explicit operator bool() const noexcept
        {
            return Iter && Iter->Last() == EReady::Data;
        }

        TIter* Get() const noexcept
        {
            return Iter.Get();
        }

        const TRemap& Remap() const noexcept
        {
            return Iter->Remap;
        }

        void Make(IPages*) noexcept
        {
            Iter = nullptr;
        }

        EReady Seek(TRawVals key, ESeek seek) noexcept
        {
            if (seek == ESeek::Upper && !key)
                Y_FAIL("Cannot cast ESeek::Upper with empty key to ELookup");

            TKeyRange range;
            range.MinKey = key;
            switch (seek) {
                case ESeek::Lower:
                    range.MinInclusive = true;
                    break;
                case ESeek::Upper:
                    range.MinInclusive = false;
                    break;
                case ESeek::Exact:
                    range.MaxKey = key;
                    range.MinInclusive = true;
                    range.MaxInclusive = true;
                    break;
            }

            if constexpr (TIter::Direction == EDirection::Reverse) {
                using namespace std;
                swap(range.MinKey, range.MaxKey);
                swap(range.MinInclusive, range.MaxInclusive);
            }

            Iter = Base.IterateRangeGeneric<TIter>(Table, range, Scheme->Tags(), Snapshot);

            return Iter->Next(Mode);
        }

        EReady Next() noexcept
        {
            return Iter->Next(Mode);
        }

        const TRowState& Apply() noexcept
        {
            return Iter->Row();
        }

    public:
        const TIntrusiveConstPtr<TRowScheme> Scheme;
        TDatabase &Base;

    private:
        const ui32 Table = Max<ui32>();
        const TRowVersion Snapshot;
        const ENext Mode;
        TAutoPtr<TIter> Iter;
    };

    using TWrapDbIter = TWrapDbIterImpl<TTableIt>;
    using TWrapDbReverseIter = TWrapDbIterImpl<TTableReverseIt>;

}
}
}
