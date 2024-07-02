#pragma once

#include "ydb/core/tablet_flat/flat_part_overlay.h"
#include <ydb/core/tablet_flat/flat_table_subset.h>
#include <ydb/core/tablet_flat/flat_iterator.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    template<class TIter>
    struct TWrapIterImpl {
        using TFrozen = decltype(TSubset::Frozen);
        using TFlatten = decltype(TSubset::Flatten);

        TWrapIterImpl(const TSubset &subset, TRowVersion snapshot = TRowVersion::Max())
            : Scheme(subset.Scheme)
            , KeyCellDefaults(Scheme->Keys)
            , Frozen(subset.Frozen)
            , Flatten(subset.Flatten)
            , Snapshot(snapshot)
            , Levels(Scheme->Keys)
        {
            TVector<const TPartView*> parts;
            parts.reserve(Flatten.size());
            for (auto &partView: Flatten) {
                Y_ABORT_UNLESS(partView.Part, "Creating TWrapIter without a part");
                Y_ABORT_UNLESS(partView.Slices, "Creating TWrapIter without slices");
                TOverlay{partView.Screen, partView.Slices}.Validate();
                parts.push_back(&partView);
            }
            std::sort(parts.begin(), parts.end(),
                [](const TPartView* a, const TPartView* b) {
                    return a->Epoch() < b->Epoch();
                });
            for (auto *p: parts) {
                Levels.Add(p->Part, p->Slices);
            }
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

        void Make(IPages *env) noexcept
        {
            Iter = nullptr, Env = env; /* Have to make on each Seek(...) */
        }

        EReady Seek(TRawVals key_, ESeek seek) noexcept
        {
            const TCelled key(key_, *Scheme->Keys, false);

            ui64 limit = seek == ESeek::Exact ? 1 : Max<ui64>();

            Iter = new TIter(&*Scheme, Scheme->Tags(), limit, Snapshot);

            for (auto &mem: Frozen)
                Iter->Push(
                    TMemIter::Make(*mem, mem.Snapshot, key, seek, KeyCellDefaults, &Iter->Remap, Env,
                        TIter::Direction));

            for (auto &run: Levels) {
                auto one = MakeHolder<TRunIter>(run, Remap().Tags, KeyCellDefaults, Env);

                EReady status;
                if constexpr (TIter::Direction == EDirection::Reverse) {
                    status = one->SeekReverse(key, seek);
                } else {
                    status = one->Seek(key, seek);
                }

                if (status != EReady::Gone)
                    Iter->Push(std::move(one));
            }

            return Iter->Next(ENext::All);
        }

        EReady Next() noexcept
        {
            return Iter->Next(ENext::All);
        }

        const TRowState& Apply() noexcept
        {
            return Iter->Row();
        }

    public:
        const TIntrusiveConstPtr<TRowScheme> Scheme;
        const TIntrusiveConstPtr<TKeyCellDefaults> KeyCellDefaults;
        const TFrozen Frozen;
        const TFlatten Flatten;
        const TRowVersion Snapshot;

    private:
        IPages *Env = nullptr;
        TLevels Levels;
        TAutoPtr<TIter> Iter;
    };

    using TWrapIter = TWrapIterImpl<TTableIter>;
    using TWrapReverseIter = TWrapIterImpl<TTableReverseIter>;

}
}
}
