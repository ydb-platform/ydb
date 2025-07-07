#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

#define UNIT_ASSERT_TIME_EQUAL(A, B, D)                                                               \
  do {                                                                                                \
    if (!(((A - B) >= TDuration::Zero()) && ((A - B) <= D))                                           \
            && !(((B - A) >= TDuration::Zero()) && ((B - A) <= D))) {                                 \
        auto&& failMsg = Sprintf("%s and %s diferent more then %s", (::TStringBuilder() << A).data(), \
            (::TStringBuilder() << B).data(), (::TStringBuilder() << D).data());                      \
        UNIT_FAIL_IMPL("assertion failure", failMsg);                                                 \
    }                                                                                                 \
  } while (false)


Y_UNIT_TEST_SUITE(WithSDK) {

    Y_UNIT_TEST(DescribeConsumer) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);

        auto describe = [&]() {
            return setup.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER);
        };

        auto write = [&](size_t seqNo) {
            TTopicClient client(setup.MakeDriver());

            TWriteSessionSettings settings;
            settings.Path(TEST_TOPIC);
            settings.PartitionId(0);
            settings.DeduplicationEnabled(false);
            auto session = client.CreateSimpleBlockingWriteSession(settings);

            TString msgTxt = TStringBuilder() << "message_" << seqNo;
            TWriteMessage msg(msgTxt);
            msg.CreateTimestamp(TInstant::Now() - TDuration::Seconds(10 - seqNo));
            UNIT_ASSERT(session->Write(std::move(msg)));

            session->Close(TDuration::Seconds(5));
        };

        // Check describe for empty topic
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetLastReadOffset());
        }

        write(3);
        write(7);

        // Check describe for topic which contains messages, but consumer hasn`t read
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(0, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag()); // 
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        UNIT_ASSERT(setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1).IsSuccess());

        // Check describe for topic whis contains messages, has commited offset but hasn`t read (restart tablet for example)
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3)); // why not zero?
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetLastReadOffset());
        }

        {
            TTopicClient client(setup.MakeDriver());
            TReadSessionSettings settings;
            settings.ConsumerName(TEST_CONSUMER);
            settings.AppendTopics(TTopicReadSettings().Path(TEST_TOPIC));

            auto session = client.CreateReadSession(settings);

            TInstant endTime = TInstant::Now() + TDuration::Seconds(5);
            while (true) {
                auto e = session->GetEvent();
                if (e) {
                    Cerr << ">>>>> Event = " << e->index() << Endl << Flush;
                }
                if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(e.value())) {
                    // we must recive only one date event with second message
                    break;
                } else if (e && std::holds_alternative<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value())) {
                    std::get<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(e.value()).Confirm();
                }
                UNIT_ASSERT_C(endTime > TInstant::Now(), "Unable wait");
            }

            session->Close(TDuration::Seconds(1));
        }

        // Check describe for topic wich contains messages, has commited offset of first message and read second message
        {
            auto d = describe();
            UNIT_ASSERT_STRINGS_EQUAL(TEST_CONSUMER, d.GetConsumer().GetConsumerName());
            UNIT_ASSERT_VALUES_EQUAL(1, d.GetPartitions().size());
            auto& p = d.GetPartitions()[0];
            UNIT_ASSERT_VALUES_EQUAL(0, p.GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(true, p.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(2, p.GetPartitionStats()->GetEndOffset());
            auto& c = p.GetPartitionConsumerStats();
            UNIT_ASSERT_VALUES_EQUAL(true, c.has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, c->GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(7), c->GetMaxWriteTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxReadTimeLag());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(0), c->GetMaxCommittedTimeLag());
            UNIT_ASSERT_TIME_EQUAL(TInstant::Now(), c->GetLastReadTime(), TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(2, c->GetLastReadOffset());
        }
    }
}

} // namespace NKikimr
