#pragma once

#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TModel2Cols : public IModel {
    public:
        TModel2Cols()
            : IModel(
                TLayoutCook()
                    .Col(0, 0,  NScheme::NTypeIds::Uint64)
                    .Col(0, 1,  NScheme::NTypeIds::Uint64)
                    .Key({ 0 }).RowScheme())
        {

        }

        TRow Make(ui64 seq, bool hole) noexcept override
        {
            TSchemedCookRow row(*Scheme);

            row.Col(seq, (Saved += hole ? 0 : 1));

            return *row;
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out << "Model2Cols";
        }

        ui64 Base(const TRow &row) const noexcept override
        {
            auto *up = row.Get(NTable::TTag(1));

            if (up == nullptr || up->Type != NScheme::NTypeIds::Uint64) {
                Y_ABORT("Probably got row not from the TMass instance");
            } else if (up->Cell.Size() != sizeof(ui64) || !up->Cell.Data()) {
                Y_ABORT("Last saved tow reference TCell is invalid in TRow");
            } else {
                return *reinterpret_cast<const ui64*>(up->Cell.Data());
            }
        }

        void Check(TArrayRef<const ui64>) const override
        {

        }

    private:
        TMersenne<ui64> Entropy = 7500;
        ui64 Saved = 0;
    };
}
}
}
