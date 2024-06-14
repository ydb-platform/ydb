#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TPartMulti) {
    using namespace NTest;

    Y_UNIT_TEST(Basics)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(0, 2,  NScheme::NTypeIds::Double)
            .Col(0, 3,  NScheme::NTypeIds::Bool)
            .Key({0, 1});

        const auto foo = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, nullptr);
        const auto bar = *TSchemedCookRow(*lay).Col(777_u32, "bar", 2.72, true);
        const auto baz = *TSchemedCookRow(*lay).Col(888_u32, "baz", 5.42, false);
        const auto zzz = *TSchemedCookRow(*lay).Col(999_u32, "zzz", 4.11, false);

        NTest::TPartEggs eggs = {
            nullptr,
            lay.RowScheme(),
            {
                TPartCook(lay, { }).Add(foo).Add(bar).Finish().Lone(),
                TPartCook(lay, { }).Add(baz).Finish().Lone(),
                TPartCook(lay, { }).Add(zzz).Finish().Lone(),
            }};

        TCheckIter wrap(eggs, { });

        wrap.To(10).Has(foo).Has(bar);
        wrap.To(11).NoVal(*TSchemedCookRow(*lay).Col(555_u32, "foo", 10.));
        wrap.To(12).NoKey(*TSchemedCookRow(*lay).Col(888_u32, "foo", 3.14));

        /*_ Basic lower and upper bounds lookup semantic  */

        wrap.To(20).Seek(foo, ESeek::Lower).Is(foo);
        wrap.To(21).Seek(foo, ESeek::Upper).Is(bar);
        wrap.To(22).Seek(bar, ESeek::Lower).Is(bar);
        wrap.To(23).Seek(bar, ESeek::Upper).Is(baz);
        wrap.To(24).Seek(baz, ESeek::Lower).Is(baz);
        wrap.To(25).Seek(baz, ESeek::Upper).Is(zzz);
        wrap.To(26).Seek(zzz, ESeek::Lower).Is(zzz);
        wrap.To(27).Seek(zzz, ESeek::Upper).Is(EReady::Gone);

        /*_ The empty key is interpreted depending on ESeek mode... */

        wrap.To(30).Seek({ }, ESeek::Lower).Is(foo);
        wrap.To(31).Seek({ }, ESeek::Exact).Is(EReady::Gone);
        wrap.To(32).Seek({ }, ESeek::Upper).Is(EReady::Gone);

        /* ... but incomplete keys are padded with +inf instead of nulls
            on lookup. Check that it really happens for Seek()'s */

        wrap.To(33).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Lower).Is(bar);
        wrap.To(34).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Upper).Is(bar);
        wrap.To(35).Seek(*TSchemedCookRow(*lay).Col(777_u32), ESeek::Lower).Is(baz);
        wrap.To(36).Seek(*TSchemedCookRow(*lay).Col(777_u32), ESeek::Upper).Is(baz);

        /*_ Next should move to the next part correctly */

        wrap.To(40).Seek(*TSchemedCookRow(*lay).Col(777_u32, "aaa"), ESeek::Lower).Is(bar)
            .To(41).Next().Is(baz)
            .To(42).Next().Is(zzz)
            .To(43).Next().Is(EReady::Gone);

        /*_ Next must not be confused by a failed exact seek */

        wrap.To(50).Seek(*TSchemedCookRow(*lay).Col(777_u32, "aaa"), ESeek::Exact).Is(EReady::Gone)
            .To(51).Next().Is(EReady::Gone);
    }

    Y_UNIT_TEST(BasicsReverse)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Col(0, 2,  NScheme::NTypeIds::Double)
            .Col(0, 3,  NScheme::NTypeIds::Bool)
            .Key({0, 1});

        const auto foo = *TSchemedCookRow(*lay).Col(555_u32, "foo", 3.14, nullptr);
        const auto bar = *TSchemedCookRow(*lay).Col(777_u32, "bar", 2.72, true);
        const auto baz = *TSchemedCookRow(*lay).Col(888_u32, "baz", 5.42, false);
        const auto zzz = *TSchemedCookRow(*lay).Col(999_u32, "zzz", 4.11, false);

        NTest::TPartEggs eggs = {
            nullptr,
            lay.RowScheme(),
            {
                TPartCook(lay, { }).Add(foo).Add(bar).Finish().Lone(),
                TPartCook(lay, { }).Add(baz).Finish().Lone(),
                TPartCook(lay, { }).Add(zzz).Finish().Lone(),
            }};

        TCheckReverseIter wrap(eggs, { });

        wrap.To(10).Has(foo).Has(bar);
        wrap.To(11).NoVal(*TSchemedCookRow(*lay).Col(555_u32, "foo", 10.));
        wrap.To(12).NoKey(*TSchemedCookRow(*lay).Col(888_u32, "foo", 3.14));

        /*_ Basic lower and upper bounds lookup semantic  */

        wrap.To(20).Seek(foo, ESeek::Lower).Is(foo);
        wrap.To(21).Seek(foo, ESeek::Upper).Is(EReady::Gone);
        wrap.To(22).Seek(bar, ESeek::Lower).Is(bar);
        wrap.To(23).Seek(bar, ESeek::Upper).Is(foo);
        wrap.To(24).Seek(baz, ESeek::Lower).Is(baz);
        wrap.To(25).Seek(baz, ESeek::Upper).Is(bar);
        wrap.To(26).Seek(zzz, ESeek::Lower).Is(zzz);
        wrap.To(27).Seek(zzz, ESeek::Upper).Is(baz);

        /*_ The empty key is interpreted depending on ESeek mode... */

        wrap.To(30).Seek({ }, ESeek::Lower).Is(zzz);
        wrap.To(31).Seek({ }, ESeek::Exact).Is(EReady::Gone);
        wrap.To(32).Seek({ }, ESeek::Upper).Is(EReady::Gone);

        /* ... but incomplete keys are padded with +inf instead of nulls
            on lookup. Check that it really happens for Seek()'s */

        wrap.To(33).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Lower).Is(foo);
        wrap.To(34).Seek(*TSchemedCookRow(*lay).Col(555_u32), ESeek::Upper).Is(foo);
        wrap.To(35).Seek(*TSchemedCookRow(*lay).Col(777_u32), ESeek::Lower).Is(bar);
        wrap.To(36).Seek(*TSchemedCookRow(*lay).Col(777_u32), ESeek::Upper).Is(bar);

        /*_ Prev should move to the previous part correctly */

        wrap.To(40).Seek(*TSchemedCookRow(*lay).Col(999_u32, "aaa"), ESeek::Lower).Is(baz)
            .To(41).Next().Is(bar)
            .To(42).Next().Is(foo)
            .To(43).Next().Is(EReady::Gone);

        /*_ Prev must not be confused by a failed exact seek */

        wrap.To(50).Seek(*TSchemedCookRow(*lay).Col(777_u32, "aaa"), ESeek::Exact).Is(EReady::Gone)
            .To(51).Next().Is(EReady::Gone);
    }
}

}
}
