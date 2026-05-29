#include "defs.h"
#include "vdisk_outofspace.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/whiteboard_flags.pb.h>
#include <util/generic/ptr.h>
#include <util/stream/null.h>
#include <util/system/thread.h>

#define STR Cerr

using namespace NKikimr;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TOutOfSpaceStateTests) {

        Y_UNIT_TEST(TestLocal) {
            TOutOfSpaceState state(8, 0);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::GREEN);

            NPDisk::TStatusFlags flags = NKikimrBlobStorage::StatusIsValid;
            state.UpdateLocalChunk(flags);
            UNIT_ASSERT_EQUAL(state.GetGlobalColor(), TSpaceColor::GREEN);
            UNIT_ASSERT_EQUAL(state.GetLocalStatusFlags(), flags);
            UNIT_ASSERT_EQUAL(state.GetGlobalStatusFlags().Flags, flags);
        }

        Y_UNIT_TEST(TestGlobal) {
            TOutOfSpaceState state(8, 3);
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

        Y_UNIT_TEST(ToWhiteboardFlag) {
            using EFlag = NKikimrWhiteboard::EFlag;

            struct TCase {
                TSpaceColor::E Color;
                EFlag Expected;
            };

            const TCase cases[] = {
                {TSpaceColor::GREEN, EFlag::Green},
                {TSpaceColor::CYAN, EFlag::Green},
                {TSpaceColor::LIGHT_YELLOW, EFlag::Yellow},
                {TSpaceColor::YELLOW, EFlag::Orange},
                {TSpaceColor::LIGHT_ORANGE, EFlag::Orange},
                {TSpaceColor::PRE_ORANGE, EFlag::Orange},
                {TSpaceColor::ORANGE, EFlag::Orange},
                {TSpaceColor::RED, EFlag::Red},
                {TSpaceColor::BLACK, EFlag::Red},
            };

            for (const auto& testCase : cases) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TOutOfSpaceState::ToWhiteboardFlag(testCase.Color),
                    testCase.Expected);
            }
        }
    }

} // NKikimr
