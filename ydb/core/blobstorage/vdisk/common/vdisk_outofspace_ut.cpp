#include "defs.h"
#include "vdisk_outofspace.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>
#include <util/stream/null.h>
#include <util/system/thread.h>

#define STR Cerr

using namespace NKikimr;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TOutOfSpaceStateTests) {

        Y_UNIT_TEST(TestLocal) {
            TOutOfSpaceState state(8, 0, "", nullptr);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::GREEN);

            NPDisk::TStatusFlags flags = NKikimrBlobStorage::StatusIsValid;
            state.UpdateLocal(flags);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::GREEN);
            UNIT_ASSERT_EQUAL(state.GetLocalStatusFlags(), flags);
            UNIT_ASSERT_EQUAL(state.GetGlobalStatusFlags().Flags, flags);
        }

        Y_UNIT_TEST(TestGlobal) {
            TOutOfSpaceState state(8, 3, "", nullptr);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::GREEN);

            NPDisk::TStatusFlags flags = NKikimrBlobStorage::StatusIsValid;
            for (int i = 0; i < 8; ++i) {
                state.Update(0, flags);
            }
            state.Update(5, flags | NKikimrBlobStorage::StatusDiskSpaceRed);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::RED);
            state.Update(4, flags | NKikimrBlobStorage::StatusDiskSpaceOrange);
            state.Update(5, flags | NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::ORANGE);
        }

        Y_UNIT_TEST(TestMetrics) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto monGroup = std::make_shared<NMonGroup::TOutOfSpaceGroup>(counters, "", "");
            TOutOfSpaceState state(8, 0, "", monGroup);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceLightYellowMove().GetAtomic(), 0);
            NPDisk::TStatusFlags flags = NKikimrBlobStorage::StatusIsValid;
            state.UpdateLocal(flags | NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceLightYellowMove().GetAtomic(), 1);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceRed().GetAtomic(), 0);
            state.UpdateLocal(flags | NKikimrBlobStorage::StatusDiskSpaceRed);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceLightYellowMove().GetAtomic(), 1);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceRed().GetAtomic(), 1);
            state.UpdateLocal(flags | NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceLightYellowMove().GetAtomic(), 2);
            UNIT_ASSERT_VALUES_EQUAL(monGroup->StatusDiskSpaceRed().GetAtomic(), 1);
        }
    }

} // NKikimr
