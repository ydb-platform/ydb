#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NKikimr::NPQ::NMLP {

    Y_UNIT_TEST_SUITE(TMLPExternalCommitTest) {
        void TestCommit(bool mlpConsumer) {
            auto setup = CreateSetup();
            {
                NYdb::NTopic::TCreateTopicSettings settings;
                settings.BeginAddSharedConsumer("mlp-consumer").KeepMessagesOrder(false).EndAddConsumer();
                settings.BeginAddConsumer("streaming-consumer").EndAddConsumer();
                CreateTopic(setup, "/Root/topic1", settings);
            }
            WriteMany(setup, "/Root/topic1", 0, 16, 113);
            TTopicClient client(setup->MakeDriver());
            auto result = client.CommitOffset("/Root/topic1", 0, mlpConsumer ? "mlp-consumer" : "streaming-consumer", 15).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), !mlpConsumer, result.GetIssues().ToString());
        }

        Y_UNIT_TEST(OffsetsCommitViaExplicitCommitRequest) {
            TestCommit(true);
        }
        Y_UNIT_TEST(OffsetsCommitViaExplicitCommitRequestStreamingConsumer) {
            TestCommit(false); // sanity check
        }
    } // Y_UNIT_TEST_SUITE(TMLPExternalCommitTest)
} // namespace NKikimr::NPQ::NMLP
