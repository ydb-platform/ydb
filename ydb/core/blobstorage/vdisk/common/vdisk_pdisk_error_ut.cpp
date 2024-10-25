#include "defs.h"
#include "vdisk_pdisk_error.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

#define STR Cerr

using namespace NKikimr;

namespace NKikimr {

    // Below are some performance tests for Lsn allocation with and without contention

    Y_UNIT_TEST_SUITE(TPDiskErrorStateTests) {

        Y_UNIT_TEST(Basic) {
            TPDiskErrorState state;
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::Good);
            UNIT_ASSERT(state.GetErrorReason().empty());

            state.Set(NKikimrProto::CORRUPTED, 0, "");
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::NoWrites);
            UNIT_ASSERT(state.GetErrorReason().empty());
        }

        Y_UNIT_TEST(Basic2) {
            TPDiskErrorState state;
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::Good);
            UNIT_ASSERT(state.GetErrorReason().empty());

            state.Set(NKikimrProto::OUT_OF_SPACE, NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation, "");
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::WriteOnlyLog);
            UNIT_ASSERT(state.GetErrorReason().empty());

            state.Set(NKikimrProto::CORRUPTED, 0, "");
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::NoWrites);
            UNIT_ASSERT(state.GetErrorReason().empty());
        }

        Y_UNIT_TEST(BasicErrorReason) {
            TPDiskErrorState state;
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::Good);
            UNIT_ASSERT(state.GetErrorReason().empty());

            state.Set(NKikimrProto::OUT_OF_SPACE, NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation, "Foo");
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::WriteOnlyLog);
            UNIT_ASSERT(state.GetErrorReason() == "Foo");

            state.Set(NKikimrProto::CORRUPTED, 0, "Bar");
            UNIT_ASSERT(state.GetState() == TPDiskErrorState::NoWrites);
            UNIT_ASSERT(state.GetErrorReason() == "Bar");
        }

    }

} // NKikimr
