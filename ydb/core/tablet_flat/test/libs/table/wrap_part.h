#pragma once

#include "test_part.h"
#include "test_iter.h"

#include <ydb/core/tablet_flat/test/libs/rows/rows.h>
#include <ydb/core/tablet_flat/flat_table_part.h>
#include <ydb/core/tablet_flat/flat_part_screen.h>
#include <ydb/core/tablet_flat/flat_part_iter.h>
#include <ydb/core/tablet_flat/flat_stat_part.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<EDirection Direction>
    struct TWrapPartImpl {
        TWrapPartImpl(const TPartEggs &eggs, TRun& run, bool defaults = true)
            : Eggs(eggs)
            , Scheme(eggs.Scheme)
            , Remap_(TRemap::Full(*Scheme))
            , Defaults(defaults)
            , State(Remap_.Size())
            , Run_(*Scheme->Keys) // unused
            , Run(run)
        {
        }
        
        TWrapPartImpl(const TPartEggs &eggs, TIntrusiveConstPtr<TSlices> slices = nullptr, bool defaults = true)
            : Eggs(eggs)
            , Scheme(eggs.Scheme)
            , Remap_(TRemap::Full(*Scheme))
            , Defaults(defaults)
            , State(Remap_.Size())
            , Run_(*Scheme->Keys)
            , Run(Run_)
        {
            if (slices || Eggs.Parts.size() == 1) {
                /* Allowed to override part slice only for lone eggs */
                AddPart(Eggs.Lone(), slices ? *slices : *Eggs.Lone()->Slices);
            } else {
                for (const auto &part : Eggs.Parts) {
                    AddPart(part, *part->Slices);
                }
            }
        }

    private:
        void AddPart(TIntrusiveConstPtr<TPart> part, const TSlices &slices)
        {
            for (const auto &slice : slices) {
                auto got = Run.FindInsertHint(part.Get(), slice);
                Y_ABORT_UNLESS(got.second, "Unexpected slices intersection");
                Run.Insert(got.first, part, slice);
            }
        }

    public:
        using TCells = TArrayRef<const TCell>;

        explicit operator bool() const noexcept
        {
            return Iter && Iter->IsValid() && Ready == EReady::Data;
        }

        TRunIter* Get() const noexcept
        {
            return Iter.Get();
        }

        const TRemap& Remap() const noexcept
        {
            return Remap_;
        }

        void Make(IPages *env) noexcept
        {
            Ready = EReady::Gone;
            Iter = MakeHolder<TRunIter>(Run, Remap_.Tags, Scheme->Keys, env);
        }

        EReady Seek(TRawVals key_, ESeek seek) noexcept
        {
            const TCelled key(key_, *Scheme->Keys, false);
            return Seek(key, seek);
        }

        EReady Seek(const TCells key, ESeek seek) noexcept
        {
            if constexpr (Direction == EDirection::Reverse) {
                Ready = Iter->SeekReverse(key, seek);
            } else {
                Ready = Iter->Seek(key, seek);
            }

            if (Ready == EReady::Data)
                Ready = RollUp();

            Y_ABORT_UNLESS(Ready != EReady::Data || Iter->IsValid());

            return Ready;
        }

        EReady SkipToRowVersion(TRowVersion rowVersion) noexcept
        {
            TIteratorStats stats;
            Ready = Iter->SkipToRowVersion(rowVersion, stats, /* committed */ nullptr, /* observer */ nullptr,
                /* decided */ ITransactionSet::None);

            if (Ready == EReady::Data)
                Ready = RollUp();

            Y_ABORT_UNLESS(Ready != EReady::Data || Iter->IsValid());

            return Ready;
        }

        TRowVersion GetRowVersion() const noexcept
        {
            Y_ABORT_UNLESS(Ready == EReady::Data);

            return Iter->GetRowVersion();
        }

        void StopAfter(TArrayRef<const TCell> key) {
            StopKey = TOwnedCellVec::Make(key);
        }

        EReady Next() noexcept
        {
            if (std::exchange(NoBlobs, false)) {
                Ready = RollUp();
            } else if (EReady::Data == (Ready = DoIterNext()))
                Ready = RollUp();

            Y_ABORT_UNLESS(Ready != EReady::Data || Iter->IsValid());

            return Ready;
        }

        const TRowState& Apply() noexcept
        {
            Y_ABORT_UNLESS(Ready == EReady::Data, "Row state isn't ready");

            return State;
        }

        EReady RollUp()
        {
            if (Defaults) {
                State.Reset(Remap_.CellDefaults());
            } else {
                State.Init(Remap_.Size());
            }

            TDbTupleRef key = Iter->GetKey();

            if (StopKey) {
                auto cmp = CompareTypedCellVectors(key.Cells().data(), StopKey.data(), Scheme->Keys->Types.data(), Min(key.Cells().size(), StopKey.size()));
                if (cmp == 0 && key.Cells().size() != StopKey.size()) {
                    // smaller key is filled with +inf => always bigger
                    cmp = key.Cells().size() < StopKey.size() ? +1 : -1;
                }
                if (Direction == EDirection::Forward && cmp > 0 || Direction == EDirection::Reverse && cmp < 0) {
                   return EReady::Gone;
                }
            }

            for (auto &pin: Remap_.KeyPins())
                State.Set(pin.Pos, ECellOp::Set, key.Columns[pin.Key]);

            Iter->Apply(State, /* committed */ nullptr, /* observer */ nullptr);

            return (NoBlobs = State.Need() > 0) ? EReady::Page : EReady::Data;
        }

        const TPartEggs Eggs;
        const TIntrusiveConstPtr<TRowScheme> Scheme;
        const TRemap Remap_;
        const bool Defaults = true;

    private:
        EReady DoIterNext() noexcept
        {
            if constexpr (Direction == EDirection::Reverse) {
                return Iter->Prev();
            } else {
                return Iter->Next();
            }
        }

        EReady Ready = EReady::Gone;
        bool NoBlobs = false;
        TRowState State;
        TRun Run_;
        TRun& Run;
        THolder<TRunIter> Iter;
        TOwnedCellVec StopKey;
    };

    using TWrapPart = TWrapPartImpl<EDirection::Forward>;
    using TWrapReversePart = TWrapPartImpl<EDirection::Reverse>;

    using TCheckIter = TChecker<TWrapPart, TPartEggs>;
    using TCheckReverseIter = TChecker<TWrapReversePart, TPartEggs>;
}
}
}
