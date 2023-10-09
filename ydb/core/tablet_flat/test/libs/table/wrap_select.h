#pragma once

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TWrapDbSelect {

        TWrapDbSelect(TDatabase &base, ui32 table, TIntrusiveConstPtr<TRowScheme> scheme,
                TRowVersion snapshot = TRowVersion::Max(),
                ui64 readTxId = 0)
            : Scheme(std::move(scheme))
            , Remap_(TRemap::Full(*Scheme))
            , Base(base)
            , Table(table)
            , Snapshot(snapshot)
            , ReadTxId(readTxId)
        {

        }

        explicit operator bool() const noexcept
        {
            return Ready == EReady::Data;
        }

        const TRowState* Get() const noexcept
        {
            return &State;
        }

        const TRemap& Remap() const noexcept
        {
            return Remap_;
        }

        void Make(IPages*) noexcept
        {
            State.Init(0);
        }

        EReady Seek(TRawVals key, ESeek seek) noexcept
        {
            Y_ABORT_UNLESS(seek == ESeek::Exact, "Db Select(...) is a point lookup");

            ITransactionMapPtr txMap;
            if (ReadTxId != 0 && Base.HasOpenTx(Table, ReadTxId)) {
                txMap = new TSingleTransactionMap(ReadTxId, TRowVersion::Min());
            }

            return (Ready = Base.Select(Table, key, Scheme->Tags(), State, /* readFlags */ 0, Snapshot, txMap));
        }

        EReady Next() noexcept
        {
            return (Ready = EReady::Gone);
        }

        const TRowState& Apply() noexcept
        {
            return State;
        }

    public:
        const TIntrusiveConstPtr<TRowScheme> Scheme;
        const TRemap Remap_;
        TDatabase &Base;

    private:
        const ui32 Table = Max<ui32>();
        const TRowVersion Snapshot;
        const ui64 ReadTxId;
        EReady Ready = EReady::Gone;
        TRowState State;
    };

}
}
}
