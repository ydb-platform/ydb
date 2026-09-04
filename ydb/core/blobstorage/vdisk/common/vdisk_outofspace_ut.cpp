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

        Y_UNIT_TEST(AsynchronousObservationsOnlyWorsenColor) {
            TOutOfSpaceState state(1, 0);
            const auto valid = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid);

            state.ObserveLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceOrange);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::ORANGE);

            state.ObserveLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceYellowStop);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::ORANGE);

            state.ObserveLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceRed);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::RED);
        }

        Y_UNIT_TEST(AuthoritativeUpdateMayImproveColor) {
            TOutOfSpaceState state(1, 0);
            const auto valid = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid);

            state.ObserveLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceRed);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::RED);

            state.UpdateLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceYellowStop);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::YELLOW);

            state.UpdateLocalChunk(valid);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::GREEN);
        }

        Y_UNIT_TEST(StaleAuthoritativeUpdateDoesNotImproveColor) {
            TOutOfSpaceState state(1, 0);
            const auto valid = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid);
            const ui64 pollGeneration = state.GetLocalSpaceObservationGeneration();

            state.ObserveLocalChunk(valid | NKikimrBlobStorage::StatusDiskSpaceOrange);
            state.UpdateLocalChunk(valid, pollGeneration);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::ORANGE);

            const ui64 nextPollGeneration = state.GetLocalSpaceObservationGeneration();
            state.UpdateLocalChunk(valid, nextPollGeneration);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::GREEN);
        }

        Y_UNIT_TEST(IdenticalObservationDoesNotInvalidatePoll) {
            TOutOfSpaceState state(1, 0);
            const auto orange = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid
                | NKikimrBlobStorage::StatusDiskSpaceOrange);
            const auto green = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid);

            state.ObserveLocalChunk(orange);
            const ui64 pollGeneration = state.GetLocalSpaceObservationGeneration();
            state.ObserveLocalChunk(orange);
            UNIT_ASSERT_VALUES_EQUAL(state.GetLocalSpaceObservationGeneration(), pollGeneration);
            state.UpdateLocalChunk(green, pollGeneration);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::GREEN);
        }

        Y_UNIT_TEST(ImprovingObservationDoesNotInvalidatePoll) {
            TOutOfSpaceState state(1, 0);
            const auto orange = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid
                | NKikimrBlobStorage::StatusDiskSpaceOrange);
            const auto green = NPDisk::TStatusFlags(NKikimrBlobStorage::StatusIsValid);

            state.ObserveLocalChunk(orange);
            const ui64 pollGeneration = state.GetLocalSpaceObservationGeneration();
            state.ObserveLocalChunk(green);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::ORANGE);
            UNIT_ASSERT_VALUES_EQUAL(state.GetLocalSpaceObservationGeneration(), pollGeneration);
            state.UpdateLocalChunk(green, pollGeneration);
            UNIT_ASSERT_EQUAL(state.GetLocalColor(), TSpaceColor::GREEN);
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
