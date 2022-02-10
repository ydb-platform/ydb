#pragma once

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TWrapDbSelect {

        TWrapDbSelect(TDatabase &base, ui32 table, TIntrusiveConstPtr<TRowScheme> scheme,
                TRowVersion snapshot = TRowVersion::Max()) 
            : Scheme(std::move(scheme))
            , Remap_(TRemap::Full(*Scheme))
            , Base(base)
            , Table(table)
            , Snapshot(snapshot) 
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
            Y_VERIFY(seek == ESeek::Exact, "Db Select(...) is a point lookup");

            return (Ready = Base.Select(Table, key, Scheme->Tags(), State, /* readFlags */ 0, Snapshot)); 
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
        EReady Ready = EReady::Gone;
        TRowState State;
    };

}
}
}
