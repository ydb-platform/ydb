#pragma once

#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

#include <util/random/mersenne.h>
#include <util/digest/city.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TModelS3Hash : public IModel {
    public:
        using TStrGen = TRandomString<TMersenne<ui64>>;

        TModelS3Hash(size_t buckets = 10)
            : IModel(
                TLayoutCook()
                    .Col(0, 0,  NScheme::NTypeIds::Uint64)
                    .Col(0, 1,  NScheme::NTypeIds::String)
                    .Col(0, 2,  NScheme::NTypeIds::String)
                    .Col(0, 8,  NScheme::NTypeIds::Uint32)
                    .Key({ 0, 1, 2 }).RowScheme())
        {
            for (size_t num : xrange(buckets)) {
                Buckets.push_back(Sprintf("Bucket_%" PRISZT, num));
            }
        }

        TRow Make(ui64 seq, bool hole) noexcept override
        {
            TSchemedCookRow row(*Scheme);

            auto &bucket = Buckets[seq % Buckets.size()];
            auto name = TStrGen(Rnd).Do(20 + Rnd.GenRand64() % 30);
            ui64 hash = CityHash64(bucket + name);

            return *row.Col(hash, bucket, name, ui32(Saved += hole ? 0 : 1));
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out << "ModelS3Hash";
        }

        ui64 Base(const TRow &row) const noexcept override
        {
            auto *up = row.Get(NTable::TTag(8));

            if (up == nullptr || up->Type != NScheme::NTypeIds::Uint32) {
                Y_ABORT("Probably got row not from the TMass instance");
            } else if (up->Cell.Size() != sizeof(ui32) || !up->Cell.Data()) {
                Y_ABORT("Last saved tow reference TCell is invalid in TRow");
            } else {
                return *reinterpret_cast<const ui32*>(up->Cell.Data());
            }
        }

        void Check(TArrayRef<const ui64>) const override
        {

        }

    private:
        TMersenne<ui64> Rnd = 7500;
        TVector<TString> Buckets;
        ui64 Saved = 0;
    };
}
}
}
