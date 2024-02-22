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
            virtual void Ver(TRowVersion rowVersion = TRowVersion::Min()) = 0;
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

            void Ver(TRowVersion rowVersion) override
            {
                Cook.Ver(rowVersion);
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

            void Ver(TRowVersion) override
            {
                Y_ABORT("unsupported");
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

        TAutoPtr<TSubset> Mixed(ui32 frozen, ui32 flatten, THash hash, float addHistory = 0, ui32 addSlices = 1)
        {
            TMersenne<ui64> rnd(0);
            TDeque<TAutoPtr<IBand>> bands;

            for (auto it: xrange(flatten)) {
                TLogoBlobID token(0, 0, ++Serial, 0, 0, 0);
                bands.emplace_back(new TPart(Scheme, TEpoch::FromIndex(bands.size()), it, Pages, token));
            }

            for (auto it: xrange(frozen))
                bands.emplace_back(new TMem(Scheme, TEpoch::FromIndex(bands.size()), it));

            if (const auto slots = bands.size()) {
                for (auto &row: Saved) {
                    auto &band = bands[hash(row) % slots];
                    if (addHistory) {
                        for (ui64 txId = 10; txId; txId--) {
                            band->Ver({0, txId});
                            // FIXME: change row data?
                            band->Add(row);
                            if (rnd.GenRandReal4() > addHistory) {
                                // each row will have from 1 to 10 versions
                                break;
                            }
                        }
                    } else {
                        band->Add(row);
                    }
                }
            }

            TAutoPtr<TSubset> subset = new TSubset(TEpoch::FromIndex(bands.size()), Scheme);
            rnd = {0};

            for (auto &one: bands) {
                if (auto *mem = dynamic_cast<TMem*>(one.Get())) {
                    auto table = mem->Cooker.Unwrap();

                    Y_ABORT_UNLESS(table->GetRowCount(), "Got empty IBand");

                    subset->Frozen.emplace_back(std::move(table), table->Immediate());
                } else if (auto *part_ = dynamic_cast<TPart*>(one.Get())) {
                    auto eggs = part_->Cook.Finish();
                    auto part = eggs.Lone();
                    TVector<TSlice> slices = *part->Slices;
                    if (addSlices > 1) {
                        slices.clear();
                        auto pages = IndexTools::CountMainPages(*part);
                        TSet<TRowId> points;
                        while (points.size() < addSlices * 2) {
                            points.insert(rnd.GenRand() % pages);
                        }
                        for (auto it = points.begin(); it != points.end(); std::advance(it, 2)) {
                            slices.push_back(IndexTools::MakeSlice(*part, *it, *next(it) + 1));
                        }
                    }
                    auto slices_ = MakeIntrusive<TSlices>(slices);
                    TPartView partView {
                        .Part = part,
                        .Screen = slices_->ToScreen(),
                        .Slices =  slices_
                    };
                    TOverlay{partView.Screen, partView.Slices}.Validate();
                    subset->Flatten.push_back(partView);
                } else {
                    Y_ABORT("Unknown IBand writer type, internal error");
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
