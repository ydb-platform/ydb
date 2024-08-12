#pragma once

#include "defs.h"
#include "flat_row_nulls.h"
#include "flat_row_celled.h"
#include "flat_part_iter.h"
#include "flat_part_screen.h"
#include "flat_part_laid.h"
#include "flat_stat_part.h"

namespace NKikimr {
namespace NTable {

    class TShrink {
    public:
        using TCells = TArrayRef<const TCell>;

        TShrink(IPages *env, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults)
            : Env(env)
            , KeyCellDefaults(keyDefaults)
        {

        }

        TShrink& Put(const TPartView &partView, TRawVals from, TRawVals to)
        {
            return Put({ &partView, 1 }, from, to);
        }

        TShrink& Put(TArrayRef<const TPartView> all, TRawVals from_, TRawVals to_)
        {
            const TCelled from(from_, *KeyCellDefaults, false);
            const TCelled to(to_, *KeyCellDefaults, false);

            return Put(all, from, to);
        }

        TShrink& Put(TArrayRef<const TPartView> all, TCells from, TCells to)
        {
            for (auto &partView: all) {
                Y_ABORT_UNLESS(partView.Slices, "Shrink attempt on a part without slices");

                if (!from && !to) /* [-inf, +inf) */ {
                    PartView.emplace_back(partView);
                } else {
                    TPartIter first(partView.Part.Get(), { }, KeyCellDefaults, Env);
                    Skipped += EReady::Page == first.Seek(from, ESeek::Lower);

                    TPartIter last(partView.Part.Get(), { }, KeyCellDefaults, Env);
                    Skipped += EReady::Page == last.Seek(to, to ? ESeek::Lower : ESeek::Upper);

                    auto firstRowId = first.GetRowId();
                    auto lastRowId = last.GetRowId();

                    if (Skipped == 0 && firstRowId < lastRowId) {
                        auto with = TScreen::THole{ firstRowId, lastRowId };
                        auto screen = TScreen::Cut(partView.Screen, with);

                        if (!screen || screen->Size() > 0) {
                            auto keys = partView.Part->Scheme->Groups[0].KeyTypes.size();

                            TArrayRef<const TCell> firstKey, lastKey;
                            firstKey = first.GetKey().Cells().Slice(0, keys);
                            if (last.IsValid()) {
                                lastKey = last.GetKey().Cells().Slice(0, keys);
                            }

                            auto run = TSlices::Cut(
                                partView.Slices,
                                firstRowId,
                                lastRowId,
                                firstKey,
                                lastKey);
                            Y_DEBUG_ABORT_UNLESS(run, "Unexpected null result");

                            if (run->size() > 0) {
                                run->Validate();
                                PartView.emplace_back(TPartView{ partView.Part, screen, std::move(run) });
                            }
                        }
                    }
                }
            }

            return *this;
        }

    public:
        IPages * const Env = nullptr;
        TIntrusiveConstPtr<TKeyCellDefaults> KeyCellDefaults;
        size_t Skipped = 0;
        TVector<TPartView> PartView;
    };

}
}
