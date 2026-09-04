#include <ydb/core/base/counters.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPTests) {

Y_UNIT_TEST(CreateWithRetentionStorage) {
    auto setup = CreateSetup();

    auto status = CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionStorageMb(1024)
            .PartitioningSettings(2, 2)
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer());

    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), false, status.GetIssues().ToString());
}

Y_UNIT_TEST(AddWithRetentionStorage) {
    auto setup = CreateSetup();

    auto status = CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionStorageMb(1024)
            .PartitioningSettings(2, 2));

    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), true, status.GetIssues().ToString());

    status = AlterTopic(setup, "/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer());

    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), false, status.GetIssues().ToString());
}

Y_UNIT_TEST(SetRetentionStorage) {
    auto setup = CreateSetup();

    auto status = CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(2, 2)
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer());

    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), true, status.GetIssues().ToString());

    status = AlterTopic(setup, "/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionStorageMb(1024));

    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), false, status.GetIssues().ToString());
}

}
}
