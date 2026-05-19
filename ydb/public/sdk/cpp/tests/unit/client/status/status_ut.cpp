#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/testing/unittest/registar.h>

#include <stdexcept>

using namespace NYdb;
using namespace NYdb::NStatusHelpers;

namespace {

TStatus CatchStatusFromCurrentException() {
    try {
        throw;
    } catch (...) {
        return StatusFromCurrentException();
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TStatusFromCurrentExceptionTest) {
    Y_UNIT_TEST(YdbErrorExceptionPreservesStatus) {
        try {
            throw TYdbErrorException(TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues{}));
        } catch (...) {
            TStatus status = CatchStatusFromCurrentException();
            UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::UNAVAILABLE);
        }
    }

    Y_UNIT_TEST(StdExceptionBecomesClientInternalError) {
        try {
            throw std::runtime_error("test failure");
        } catch (...) {
            TStatus status = CatchStatusFromCurrentException();
            UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::CLIENT_INTERNAL_ERROR);
            UNIT_ASSERT(status.GetIssues());
        }
    }

    Y_UNIT_TEST(UnknownExceptionBecomesClientInternalError) {
        try {
            throw 42;
        } catch (...) {
            TStatus status = CatchStatusFromCurrentException();
            UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::CLIENT_INTERNAL_ERROR);
        }
    }
}
