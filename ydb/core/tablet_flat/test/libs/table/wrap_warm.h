#pragma once

#include <ydb/core/tablet_flat/test/libs/rows/rows.h>

#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_mem_iter.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<EDirection Direction>
    struct TWrapMemtableImpl {
        TWrapMemtableImpl(TIntrusiveConstPtr<TMemTable> egg, bool defaults = true)
            : Table(egg)
            , Scheme(Table->Scheme)
            , Remap_(TRemap::Full(*Scheme))
            , Defaults(defaults)
            , State(Remap_.Size())
        {

        }

        explicit operator bool() const noexcept
        {
            return Iter && Iter->IsValid();
        }

        TMemIter* Get() const noexcept
        {
            return Iter.Get();
        }

        const TRemap& Remap() const noexcept
        {
            return Remap_;
        }

        void Make(IPages *env) noexcept
        {
            Iter = nullptr, Env = env; /* Have to make on each Seek(...) */
        }

        EReady Seek(TRawVals key_, ESeek seek) noexcept
        {
            const TCelled key(key_, *Scheme->Keys, false);

            Iter = TMemIter::Make(*Table, Table->Immediate(), key, seek, Scheme->Keys, &Remap_, Env, Direction);

            return RollUp();
        }

        EReady Next() noexcept
        {
            if constexpr (Direction == EDirection::Reverse) {
                Iter->Prev();
            } else {
                Iter->Next();
            }

            return RollUp();
        }

        const TRowState& Apply() noexcept
        {
            Y_ABORT_UNLESS(*this, "Iterator isn't ready");

            return State;
        }

        EReady RollUp()
        {
            if (Iter->IsValid()) {
                if (Defaults) {
                    State.Reset(Remap_.CellDefaults());
                } else {
                    State.Init(Remap_.Size());
                }

                TDbTupleRef key = Iter->GetKey();

                for (auto &pin: Remap_.KeyPins())
                    State.Set(pin.Pos, ECellOp::Set, key.Columns[pin.Key]);

                Iter->Apply(State, /* committed */ nullptr, /* observer */ nullptr);
            }

            return Iter->IsValid() ? EReady::Data : EReady::Gone;
        }

        const TIntrusiveConstPtr<TMemTable> Table;
        const TIntrusiveConstPtr<TRowScheme> Scheme;
        const TRemap Remap_;
        const bool Defaults = true;

    private:
        IPages *Env = nullptr;
        TRowState State;
        TAutoPtr<TMemIter> Iter;
    };

    using TWrapMemtable = TWrapMemtableImpl<EDirection::Forward>;
    using TWrapReverseMemtable = TWrapMemtableImpl<EDirection::Reverse>;

}
}
}
