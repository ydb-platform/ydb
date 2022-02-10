#pragma once

#include "test_part.h"
#include "test_writer.h"
#include "test_cooker.h"
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/tablet_flat/flat_row_nulls.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TMake {
        using TScheme = TRowScheme;

        struct IBand {
            virtual ~IBand() = default;
            virtual void Add(const TRow&) noexcept = 0;
        };

        struct TPart : IBand {
            TPart(TIntrusiveConstPtr<TScheme> scheme, TEpoch epoch, ui32, NPage::TConf conf, const TLogoBlobID &token)
                : Epoch(epoch)
                , Cook(std::move(scheme), conf, token, epoch)
            {

            }

            void Add(const TRow &row) noexcept override
            {
                Cook.Add(row);
            }

            const TEpoch Epoch;
            TPartCook Cook;
        };

        struct TMem : IBand {
            TMem(TIntrusiveConstPtr<TScheme> scheme, TEpoch epoch, ui32)
                : Cooker(std::move(scheme), epoch)
            {

            }

            void Add(const TRow &row) noexcept override
            {
                Cooker.Add(row, ERowOp::Upsert);
            }

            TCooker Cooker;
        };

        using THash = std::function<ui32(const TRow&)>;

        TMake(const NTest::TMass &mass, NPage::TConf pages = { })
            : Pages(pages)
            , Saved(mass.Saved)
            , Scheme(mass.Model->Scheme)
        {

        }

        TIntrusivePtr<TMemTable> Mem()
        {
            return TCooker(Scheme, TEpoch::Zero()).Add(Saved, ERowOp::Upsert).Unwrap();
        }

        TPartEggs Part()
        {
            TPartCook cook(Scheme, Pages);

            return cook.Add(Saved.begin(), Saved.end()).Finish();
        }

        TAutoPtr<TSubset> Mixed(ui32 frozen, ui32 flatten, THash hash)
        {
            TDeque<TAutoPtr<IBand>> bands;

            for (auto it: xrange(flatten)) {
                TLogoBlobID token(0, 0, ++Serial, 0, 0, 0);
                bands.emplace_back(new TPart(Scheme, TEpoch::FromIndex(bands.size()), it, Pages, token));
            }

            for (auto it: xrange(frozen))
                bands.emplace_back(new TMem(Scheme, TEpoch::FromIndex(bands.size()), it));

            if (const auto slots = bands.size()) {
                for (auto &row: Saved)
                    bands[hash(row) % slots]->Add(row);
            }

            TAutoPtr<TSubset> subset = new TSubset(TEpoch::FromIndex(bands.size()), Scheme);

            for (auto &one: bands) {
                if (auto *mem = dynamic_cast<TMem*>(one.Get())) {
                    auto table = mem->Cooker.Unwrap();

                    Y_VERIFY(table->GetRowCount(), "Got empty IBand");

                    subset->Frozen.emplace_back(std::move(table), table->Immediate());
                } else if (auto *part_ = dynamic_cast<TPart*>(one.Get())) {
                    auto eggs = part_->Cook.Finish();

                    if (eggs.Parts.size() != 1) {
                        Y_Fail("Unexpected " << eggs.Parts.size() << " parts");
                    }

                    subset->Flatten.push_back(
                                { eggs.At(0), nullptr, eggs.At(0)->Slices });
                } else {
                    Y_FAIL("Unknown IBand writer type, internal error");
                }
            }

            return subset;
        }

    private:
        const NPage::TConf Pages { true, 7 * 1024 };
        ui32 Serial = 0;
        const TRowsHeap &Saved;
        TIntrusiveConstPtr<TScheme> Scheme;
    };

}
}
}
