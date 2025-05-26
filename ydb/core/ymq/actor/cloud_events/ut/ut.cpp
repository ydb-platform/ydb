#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include <ydb/core/ymq/actor/cloud_events/cloud_events.h>

Y_UNIT_TEST_SUITE(AuditCloudEvents) {

    Y_UNIT_TEST(BaseCerr) {
        std::cout << "Hello =)" << std::endl;
    }
}
