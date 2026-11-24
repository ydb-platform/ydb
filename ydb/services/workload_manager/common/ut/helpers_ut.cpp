#include <ydb/services/workload_manager/common/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NWorkloadManager {

Y_UNIT_TEST_SUITE(WorkloadManagerHelpers) {
    Y_UNIT_TEST(IsServerlessDatabaseId) {
        UNIT_ASSERT(IsServerlessDatabaseId("72075186224037888:5:/Root/serverless"));
        UNIT_ASSERT(IsServerlessDatabaseId("1:2:/Root/a:b"));

        UNIT_ASSERT(!IsServerlessDatabaseId("/Root/dedicated"));
        UNIT_ASSERT(!IsServerlessDatabaseId("/Root"));
        UNIT_ASSERT(!IsServerlessDatabaseId("/Root/a:b"));
        UNIT_ASSERT(!IsServerlessDatabaseId("Root/db"));
        UNIT_ASSERT(!IsServerlessDatabaseId("1:2:"));
        UNIT_ASSERT(!IsServerlessDatabaseId(""));
    }
}

}   // namespace NKikimr::NWorkloadManager
