#pragma once

#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<class TIter>
    struct TWrapDbIterImpl {

        TWrapDbIterImpl(TDatabase &base, ui32 table, TIntrusiveConstPtr<TRowScheme> scheme,
                TRowVersion snapshot = TRowVersion::Max(),
                ui64 readTxId = 0,
                ENext mode = ENext::All)
            : Scheme(std::move(scheme))
            , Base(base)
            , Table(table)
            , Snapshot(snapshot)
            , ReadTxId(readTxId)
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
                Y_ABORT("Cannot cast ESeek::Upper with empty key to ELookup");

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

            ITransactionMapPtr txMap;
            if (ReadTxId != 0 && Base.HasOpenTx(Table, ReadTxId)) {
                txMap = new TSingleTransactionMap(ReadTxId, TRowVersion::Min());
            }

            Iter = Base.IterateRangeGeneric<TIter>(Table, range, Scheme->Tags(), Snapshot, txMap);

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
        const ui64 ReadTxId;
        const ENext Mode;
        TAutoPtr<TIter> Iter;
    };

    using TWrapDbIter = TWrapDbIterImpl<TTableIter>;
    using TWrapDbReverseIter = TWrapDbIterImpl<TTableReverseIter>;

}
}
}
