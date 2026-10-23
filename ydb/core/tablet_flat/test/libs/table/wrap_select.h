#pragma once

#include "wrap_dbase.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TWrapDbSelect {

        TWrapDbSelect(TDatabase &base, ui32 table, TIntrusiveConstPtr<TRowScheme> scheme,
                TRowVersion snapshot = TRowVersion::Max(),
                ui64 readTxId = 0,
                ui32 readMaxVisibleSavepointSeqNum = Max<ui32>())
            : Scheme(std::move(scheme))
            , Remap_(TRemap::Full(*Scheme))
            , Base(base)
            , Table(table)
            , Snapshot(snapshot)
            , ReadTxId(readTxId)
            , ReadMaxVisibleSavepointSeqNum(readMaxVisibleSavepointSeqNum)
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

        void Make(IPages*)
        {
            State.Init(0);
        }

        EReady Seek(TRawVals key, ESeek seek)
        {
            Y_ENSURE(seek == ESeek::Exact, "Db Select(...) is a point lookup");

            auto txMap = MakeReadTxMap(Base, Table, ReadTxId, ReadMaxVisibleSavepointSeqNum);

            return (Ready = Base.Select(Table, key, Scheme->Tags(), State, /* readFlags */ 0, Snapshot, txMap));
        }

        EReady Next()
        {
            return (Ready = EReady::Gone);
        }

        const TRowState& Apply()
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
        const ui32 ReadMaxVisibleSavepointSeqNum;
        EReady Ready = EReady::Gone;
        TRowState State;
    };

}
}
}
