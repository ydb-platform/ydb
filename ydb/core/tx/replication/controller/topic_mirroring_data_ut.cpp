#include "topic_mirroring_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {
namespace {

Y_UNIT_TEST_SUITE(TopicMirroringData) {
    Y_UNIT_TEST(DestinationConsumerCommitsAreDisabledWhileMirroring) {
        TTopicMirroringFixture test;
        test.ReplicateFirstMessage();

        NYdb::NTopic::TTopicClient destination(test.Destination.GetDriver());
        const auto commit = destination.CommitOffset(test.DestinationTopic, 0, test.Consumer, 1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(commit.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(commit.GetIssues().ToString(), "Commits for consumer are disabled");

        test.CommitAndWaitForConsumerOffset(1);
    }

    Y_UNIT_TEST(MirrorConsumerReattachesAfterSourceTabletRestart) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("before-restart", 0);
        test.RestartSourceTopicTablet();
        test.WriteAndReadBack("after-restart", 1);
        test.WaitForOffsets(2, 0);
        test.CommitAndWaitForConsumerOffset(2);
    }

    Y_UNIT_TEST(MirroredMessagesPreserveProducerIdentityAndTimestamps) {
        using namespace NYdb::NTopic;
        TTopicMirroringFixture test;
        TTopicClient client(test.Source.GetDriver());
        auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(test.SourceTopic).ProducerId("identity-producer").MessageGroupId("identity-producer").Codec(ECodec::RAW));
        const auto createTime = TInstant::MilliSeconds(TInstant::Now().MilliSeconds() - 5000);
        UNIT_ASSERT(writer->Write(TWriteMessage("identity").SeqNo(42).CreateTimestamp(createTime)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

        const auto messages = test.ReadMessages(0, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Data, "identity");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].ProducerId, "identity-producer");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].SeqNo, 42);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].CreateTime, createTime);
        UNIT_ASSERT(messages[0].WriteTime > TInstant::Zero());
    }

    Y_UNIT_TEST(MirrorsUserMessageMeta) {
        using namespace NYdb::NTopic;
        TTopicMirroringFixture test;
        TTopicClient client(test.Source.GetDriver());
        auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(test.SourceTopic).ProducerId("meta-producer").MessageGroupId("meta-producer").Codec(ECodec::RAW));
        UNIT_ASSERT(writer->Write(TWriteMessage("with-meta").MessageMeta({{"user-key", "user-value"}})));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

        const auto messages = test.ReadMessages(0, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        const auto& meta = messages[0].Meta;
        const auto it = FindIf(meta, [](const auto& field) {
            return field.first == "user-key";
        });
        UNIT_ASSERT_C(it != meta.end(), "User message meta must survive mirroring");
        UNIT_ASSERT_VALUES_EQUAL(it->second, "user-value");
    }

    Y_UNIT_TEST(MirrorsCompressedMessages) {
        TTopicMirroringFixture test;
        const TString payload = TString(100000, 'a');
        using namespace NYdb::NTopic;
        TTopicClient client(test.Source.GetDriver());
        auto writer = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(test.SourceTopic).ProducerId("gzip").MessageGroupId("gzip").Codec(ECodec::GZIP));
        UNIT_ASSERT(writer->Write(payload));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
        test.ReadBack({payload}, 0);
        test.WaitForOffsets(1, 0);
    }

    Y_UNIT_TEST(MirrorsLargeMessage) {
        TTopicMirroringFixture test;
        const TString payload = TString(8 * 1024 * 1024, 'x');
        test.Write({payload});
        test.ReadBack({payload}, 0);
        test.WaitForOffsets(1, 0);
    }

    Y_UNIT_TEST(AllSourceConsumerOffsetsAreMirrored) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        NYdb::NTopic::TTopicClient source(test.Source.GetDriver());
        const auto altered = source.AlterTopic(test.SourceTopic, NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer("other").EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());

        test.Start("localhost");
        test.Write({"first", "second"});
        test.WaitForOffsets(2, 0);
        test.CommitAndWaitForConsumerOffset(2);
        test.CommitAndWaitForConsumerOffset(1, "other");
    }

    Y_UNIT_TEST(CommitAheadOfDeliveryIsClamped) {
        TMirrorReaderFactory mirrorReader;
        TTopicMirroringFixture test("[::1]", false, &mirrorReader);
        test.ReplicateFirstMessage();

        mirrorReader.Pause();
        Y_DEFER { mirrorReader.Resume(); };
        test.Write({"second", "third"});
        const auto commit = test.CommitSourceConsumerOffset(3);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(test.GetOffsets(test.Destination, test.DestinationTopic, test.Consumer).Committed, 0);

        mirrorReader.Resume();
        test.WaitForOffsets(3, 3);
    }

    Y_UNIT_TEST(CommitsDuringPauseCatchUpAfterResume) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("first", 0);
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        test.WaitMirror(false);
        const auto commit = test.CommitSourceConsumerOffset(1);
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        test.Exec("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'STANDBY');");
        test.WaitMirror(true);
        test.CommitAndWaitForConsumerOffset(1);
        test.WriteAndReadBack("second", 1);
    }

    Y_UNIT_TEST(MirroringResumesAfterControllerRestart) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("before-restart", 0);
        test.RestartController();
        test.WriteAndReadBack("after-restart", 1);
        test.WaitForOffsets(2, 0);
        test.CommitAndWaitForConsumerOffset(2);
    }

    Y_UNIT_TEST(MirroringResumesAfterDestinationTabletRestart) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("before-restart", 0);
        test.RestartDestinationTopicTablet();
        test.WriteAndReadBack("after-restart", 1);
        test.WaitForOffsets(2, 0);
    }

    Y_UNIT_TEST(MirrorConsumerRecreatedAfterManualDrop) {
        TTopicMirroringFixture test;
        test.WriteAndReadBack("before-drop", 0);
        NYdb::NTopic::TTopicClient source(test.Source.GetDriver());
        const auto altered = source.AlterTopic(test.SourceTopic, NYdb::NTopic::TAlterTopicSettings()
            .DropConsumers({test.MirrorConsumer.c_str()})).GetValueSync();
        UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());

        test.WriteAndReadBack("after-drop", 1);
        test.WaitForOffsets(2, 0);
    }
}
}
}
