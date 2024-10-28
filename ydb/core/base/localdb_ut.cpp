#include "localdb.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NLocalDb {

    Y_UNIT_TEST_SUITE(TLocalDbTest) {

        Y_UNIT_TEST(BackupTaskNameChangedAtLoadTime) {
            NKikimrCompaction::TCompactionPolicy policyPb;
            CreateDefaultUserTablePolicy()->Serialize(policyPb);

            // We expect default task name to be "scan" at the moment
            UNIT_ASSERT_VALUES_EQUAL(policyPb.GetBackupResourceBrokerTask(), "scan");

            // There are a lot of policies out there with "compaction_gen1" as the backup task name
            // We expect it to automatically change to "scan" at load time
            {
                policyPb.SetBackupResourceBrokerTask("compaction_gen1");
                TCompactionPolicy policy(policyPb);
                UNIT_ASSERT_VALUES_EQUAL(policy.BackupResourceBrokerTask, "scan");
            }

            // For other non-legacy task names we expect it to stay unchanged
            {
                policyPb.SetBackupResourceBrokerTask("backup");
                TCompactionPolicy policy(policyPb);
                UNIT_ASSERT_VALUES_EQUAL(policy.BackupResourceBrokerTask, "backup");
            }
        }

    } // Y_UNIT_TEST_SUITE(TLocalDbTest)

} // namespace NLocalDb
} // namespace NKikimr
