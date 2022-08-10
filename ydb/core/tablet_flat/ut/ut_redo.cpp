#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>

namespace NKikimr {
namespace NTable {


Y_UNIT_TEST_SUITE(Redo) {
    namespace ETypes = NScheme::NTypeIds;

    using namespace NTest;

    Y_UNIT_TEST(ABI_008)
    {
        /* Redo log taken from DBase::Basics test that cover all events */

        TLayoutCook lay;

        lay
            .Col(0, 1,  ETypes::String)
            .Col(0, 4,  ETypes::Uint64, Cimple(77_u64))
            .Col(0, 5,  ETypes::String)
            .Key({ 1 });

        const auto foo = *TSchemedCookRow(*lay).Col("foo", 33_u64);
        const auto bar = *TSchemedCookRow(*lay).Col("bar", 11_u64);

        const auto raw = NResource::Find("abi/008_basics_db.redo");

        TStringInput input(raw);

        auto changes = TDbExec::RestoreChanges(input);

        UNIT_ASSERT_C(changes.size() == 9, "Unexpected changes in test data");

        TDbExec me;
        ui32 serial = 0;

        for (auto &one: changes) {
            me.To(++serial).Cleanup()->RollUp(one->Stamp, one->Scheme, one->Redo, { });

            if (1 == serial) {
                /* Just applied initial alter script, nothing to check */

                UNIT_ASSERT(me->Subset(1, TEpoch::Max(), { }, { })->Head == TEpoch::FromIndex(1));

            } else if (2 == serial) {
                me.Iter(1).Has(foo).HasN(nullptr, 77_u64).NoKey(bar);
            } else if (3 == serial) {
                me.Iter(1).Has(foo).Has(bar);
            } else if (4 == serial) {
                /* Table flush (snapshot) event received with scheme updates */

                UNIT_ASSERT(me->Subset(1, TEpoch::Max(), { }, { })->Head == TEpoch::FromIndex(2));

            } else if (5 == serial) {
                me.Iter(1, false).NoKey(foo).NoVal(bar);
                me.Iter(1, false).HasN("bar", 11_u64, "yo");
            } else if (6 == serial) {
                me.Iter(1, false).HasN("bar", 77_u64, "me");
            } else if (7 == serial) {
                me.Iter(1, false).HasN("bar", 99_u64, nullptr);
            } else if (8 == serial) {
                me.Iter(1, false).HasN("bar", nullptr, "eh");
            } else if (9 == serial) {
                me.Iter(1, false).Has(bar).NoKey(foo);
            } else {
                UNIT_FAIL("Got to many change entries in redo samples");
            }
        }
    }
}


}
}
