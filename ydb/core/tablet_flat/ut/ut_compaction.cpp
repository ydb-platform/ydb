#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_comp.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TCompaction) {

    Y_UNIT_TEST(OneMemtable)
    {
        using namespace NTest;

        const TMass mass(new TModelStd(false), 16);

        auto eggs = TCompaction().Do(TMake(mass).Mem());

        TCheckIter(eggs, { }).IsTheSame(mass.Saved);
    }

    Y_UNIT_TEST(ManyParts)
    {
        using namespace NTest;

        const TMass mass(new TModelStd(false), 32 * 1025);

        auto subset = TMake(mass).Mixed(0, 19, NTest::TMixerRnd(19));
        auto eggs = TCompaction().Do(*subset);

        TCheckIter(eggs, { }).IsTheSame(mass.Saved);
    }

    Y_UNIT_TEST(BootAbort)
    {
        using namespace NTest;

        TMersenne<ui64> rnd;

        const TMass mass(new TModelStd(false), 32 * 1025);
        auto subset = TMake(mass).Mixed(0, 19, NTest::TMixerRnd(19));

        TAutoPtr<IPages> env = new TFailEnv<decltype(rnd)>(rnd, -1.);

        auto eggs = TCompaction(env, { }, 1 /* early abort */).Do(*subset);

        UNIT_ASSERT(!eggs.Parts && eggs.NoResult());
    }

    Y_UNIT_TEST(Defaults)
    {
        using namespace NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 8,  NScheme::NTypeIds::Uint32, Cimple<ui32>(7))
            .Key({ 0 });

        auto one = *TSchemedCookRow(*lay).Col<ui64,ui32>(1, 3);

        auto eggs =
            TPartCook(lay, { false, 4096 })
                .Add(one)               /* full row         */
                .AddN(2_u64, nullptr)   /* explciit null    */
                .AddN(3_u64).Finish();  /* default value    */

        { /* Read without default values expansion  */

            auto born = TCompaction(nullptr, { true, 7 * 1024 }).Do(eggs);

            TCheckIter
                (born, { nullptr, 0 }, nullptr , false)
                .To(10).Seek({ }, ESeek::Lower).Is(one)
                .To(11).Next().IsN(2_u64, nullptr)  /* expllcit null */
                .To(12).Next().IsN(3_u64, nullptr)  /* ommited value */
                .To(19).Next().Is(EReady::Gone);
        }

        { /* Read with expansion ommited to defaults */

            auto born = TCompaction(nullptr, { true, 7 * 1024 }).Do(eggs);

            TCheckIter
                (born, { nullptr, 0 }, nullptr, true)
                .To(20).Seek({ }, ESeek::Lower).Is(one)
                .To(21).Next().IsN(2_u64, nullptr)
                .To(22).Next().IsN(3_u64, 7_u32)
                .To(29).Next().Is(EReady::Gone);
        }
    }

    Y_UNIT_TEST(Merges)
    {
        using namespace NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 8,  NScheme::NTypeIds::Uint32, Cimple<ui32>(5))
            .Col(0, 9,  NScheme::NTypeIds::Uint32, Cimple<ui32>(7))
            .Key({ 0 });

        auto lower =
            TPartCook(lay, { false, 4096 }, { }, TEpoch::FromIndex(1))
                .AddN(1_u64, 1_u32, 1_u32)
                .AddN(2_u64, 2_u32, 3_u32)
                .AddN(3_u64, 3_u32, 6_u32)
                .AddN(4_u64, 4_u32, 8_u32)
                .Finish();

        auto middle =
            TPartCook(lay, { false, 4096 }, { }, TEpoch::FromIndex(2))
                .AddOpN(ERowOp::Erase, 2_u64)
                .AddOpN(ERowOp::Erase, 3_u64)
                .AddOpN(ERowOp::Reset, 4_u64, 9_u32, nullptr)
                .Finish();

        auto upper =
            TPartCook(lay, { false, 4096 }, { }, TEpoch::FromIndex(3))
                .AddOpN(ERowOp::Erase, 2_u64)
                .AddOpN(ERowOp::Upsert, 3_u64, ECellOp::Empty, nullptr)
                .AddOpN(ERowOp::Upsert, 4_u64, ECellOp::Empty, ECellOp::Reset)
                .Finish();

        { /* full parts merge, check final results only */
            TCompaction make(nullptr, { true, 7 * 1024 });

            auto born = make.Do(lower.Scheme, { &lower, &middle, &upper });

            TCheckIter
                (born, { nullptr, 0 }, nullptr, true /* expand defaults */)
                .To(20).Seek({ }, ESeek::Lower).Is(EReady::Data)
                .To(21).IsOpN(ERowOp::Upsert, 1_u64, 1_u32, 1_u32)
                .Next()
                .To(22).IsOpN(ERowOp::Reset, 3_u64, ECellOp::Empty, nullptr)
                .To(23).IsOpN(ERowOp::Reset, 3_u64, 5_u32, nullptr)
                .Next()
                .To(24).IsOpN(ERowOp::Reset, 4_u64, 9_u32, ECellOp::Empty)
                .To(25).IsOpN(ERowOp::Reset, 4_u64, 9_u32, 7_u32)
                .To(29).Next().Is(EReady::Gone);
        }

        { /* partial merge, check intermediate states */
            TCompaction make(nullptr, { false, 7 * 1024 });

            auto born = make.Do(lower.Scheme, { &middle, &upper });

            TCheckIter
                (born, { nullptr, 0 }, nullptr, true /* expand defaults */)
                .To(30).Seek({ }, ESeek::Lower).Is(EReady::Data)
                .To(31).IsOpN(ERowOp::Erase, 2_u64, ECellOp::Empty, ECellOp::Empty)
                .Next()
                .To(32).IsOpN(ERowOp::Reset, 3_u64, ECellOp::Empty, nullptr)
                .To(33).IsOpN(ERowOp::Reset, 3_u64, 5_u32, nullptr)
                .Next()
                .To(34).IsOpN(ERowOp::Reset, 4_u64, 9_u32, ECellOp::Reset)
                .To(35).IsOpN(ERowOp::Reset, 4_u64, 9_u32, 7_u32)
                .To(39).Next().Is(EReady::Gone);
        }
    }
}

}
}
