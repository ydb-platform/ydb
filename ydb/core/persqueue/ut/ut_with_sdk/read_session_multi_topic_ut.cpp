#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/persqueue/ut/common/raw_read_session.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NKikimr {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NKikimr::NPQ::NTest;

    namespace {

        // Partition ids are local to a topic: both topics have partition 0.
        const TString TopicA = "topic-a";
        const TString TopicB = "topic-b";

        TTopicSdkTestSetup CreateTwoTopicsSetup() {
            TTopicSdkTestSetup setup = CreateSetup();
            setup.CreateTopic(TopicA, TEST_CONSUMER, 1, 100);
            setup.CreateTopic(TopicB, TEST_CONSUMER, 1, 100);
            return setup;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(ReadSessionMultiTopic) {

        Y_UNIT_TEST(EndPartitionSessionOfEachTopic) {
            TTopicSdkTestSetup setup = CreateTwoTopicsSetup();
            ui64 txId = 1006;
            SplitPartition(setup.GetRuntime(), ++txId, TopicA, 0, "a");
            SplitPartition(setup.GetRuntime(), ++txId, TopicB, 0, "a");

            TRawReadSession reader(setup, {.Topics = {TopicA, TopicB}, .AutoPartitioningSupport = true});
            TVector<i64> expected = {reader.WaitPartitionSession(TopicA, 0), reader.WaitPartitionSession(TopicB, 0)};
            reader.WaitFor([&]() { return reader.GetEndedPartitionSessions().size() >= expected.size(); }, "EndPartitionSession of both topics");
            TVector<i64> ended = reader.GetEndedPartitionSessions();
            Sort(ended);
            Sort(expected);
            UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", ended), JoinSeq(",", expected));
        }

        // Partition 0 of the other topic is still being read. It is not a parent of the child and must not block the child commit.
        Y_UNIT_TEST_TWIN(ChildCommitIgnoresOtherTopicPartition, AutoPartitioningSupport) {
            TTopicSdkTestSetup setup = CreateTwoTopicsSetup();
            ui64 txId = 1006;
            SplitPartition(setup.GetRuntime(), ++txId, TopicA, 0, "a");
            TTopicClient client = setup.MakeClient();
            std::shared_ptr<ISimpleBlockingWriteSession> writer = CreateWriteSession(client, "producer-1", 1, TopicA, false);
            UNIT_ASSERT(writer->Write(Msg("child", 1)));
            UNIT_ASSERT(writer->Close(TDuration::Seconds(10)));

            // Topic A starts reading only after the session holds partition 0 of topic B.
            TRawReadSession reader(setup, {.Topics = {TopicA, TopicB}, .AutoPartitioningSupport = AutoPartitioningSupport, .DeferredStarts = {{TopicA, 0}}});
            reader.WaitPartitionSession(TopicB, 0);
            reader.ConfirmStart(TopicA, 0);
            const TRawReadSession::TMessage child = reader.ReadMessage(TopicA, 1);
            reader.Commit(child);
            reader.WaitCommitted(TopicA, 1, child.Offset + 1);
        }

    } // Y_UNIT_TEST_SUITE(ReadSessionMultiTopic)

} // namespace NKikimr
