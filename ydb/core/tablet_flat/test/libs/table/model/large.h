#pragma once

#include <ydb/core/tablet_flat/test/libs/rows/mass.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <util/random/mersenne.h>
#include <array>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TModelStd : public IModel {
    public:
        using TRnd = TMersenne<ui64>;

        TModelStd(bool groups)
            : IModel(CookLayout(groups).RowScheme())
        {

        }

        TRow Make(ui64, bool hole) noexcept override
        {
            const ui64 up = hole ? 0 : 1;

            TSchemedCookRow row(*Scheme);

            {
                Sub[0] += up, row.Col(Seq * 3 + 7, ui32(Seq + 10), Sub[0]);
            }

            if (Entropy.GenRandReal4() < 0.500) {
                auto ln = TRandomString<TRnd>(Entropy).Do(1 + (Seq & 0x1f));

                Sub[1] += up, row.To(3).Col<TString, bool>(ln, bool(Seq & 2));
            }

            if (Entropy.GenRandReal4() < 0.250) {
                auto shift = Seq & 0xf;

                Sub[2] += up, row.To(5).Col<ui32, ui32>(shift, Seq << shift);
            }

            if (Entropy.GenRandReal4() < 0.125) {
                auto ln = TRandomString<TRnd>(Entropy).Do(7 + (Seq & 0x7f));

                Sub[3] += up, row.To(7).Col<TString, ui64>(ln, ln.size());
            }

            return Seq++, *row;
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out
                << "Std{"
                << Sub[0] << ", " << Sub[1] << ", "
                << Sub[2] << ", " << Sub[3] << "}";
        }

        static TLayoutCook CookLayout(bool groups)
        {
            return
                TLayoutCook()
                    .Col(groups ? 0 : 0, 0,  NScheme::NTypeIds::Uint64)
                    .Col(groups ? 0 : 0, 1,  NScheme::NTypeIds::Uint32)
                    .Col(groups ? 0 : 0, 2,  NScheme::NTypeIds::Uint64)
                    .Col(groups ? 1 : 0, 3,  NScheme::NTypeIds::String)
                    .Col(groups ? 1 : 0, 4,  NScheme::NTypeIds::Bool)
                    .Col(groups ? 5 : 0, 5,  NScheme::NTypeIds::Uint32)
                    .Col(groups ? 5 : 0, 6,  NScheme::NTypeIds::Uint32)
                    .Col(groups ? 8 : 0, 7,  NScheme::NTypeIds::String)
                    .Col(groups ? 8 : 0, 8,  NScheme::NTypeIds::Uint64)
                    .Key({ 0, 1 });
        }

        ui64 Base(const TRow &row) const noexcept override
        {
            auto *up = row.Get(NTable::TTag(2));

            if (up == nullptr || up->Type != NScheme::NTypeIds::Uint64) {
                Y_ABORT("Probably got row not from the TMass instance");
            } else if (up->Cell.Size() != sizeof(ui64) || !up->Cell.Data()) {
                Y_ABORT("Last saved tow reference TCell is invalid in TRow");
            } else {
                return up->Cell.AsValue<ui64>();
            }
        }

        void Check(TArrayRef<const ui64> rows) const override
        {
            Y_ABORT_UNLESS(rows.size() == 1);

            if (rows[0] != Sub[0]) {
                throw
                    yexception()
                        << "TMass part has " << rows[0] << " rows"
                        << ", but should be " << Sub[0] << " rows";
            }
        }

    private:
        TRnd Entropy = 7500;
        ui64 Seq = 0;
        std::array<ui64, 4> Sub {{ 0, 0, 0, 0 }};
    };

}
}
}
