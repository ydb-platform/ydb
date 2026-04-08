#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/txusage_fixture.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

namespace {

constexpr const char* TRACK_PRODUCER_ID_IN_TX_META_KEY = "track_producer_id_in_tx";

/// Session meta `track_producer_id_in_tx` for Topic API write session (see write_session_actor).
enum class ETrackProducerIdInTxMeta {
    Absent,
    True,
    False,
};

class TFixtureTopicTxMatrixBase : public TFixtureTable {
protected:
    /// Two table sessions, same producer id, two writes per tx, both write sessions closed before any commit.
    /// First commit is always SUCCESS; \p expectedTx2CommitStatus is asserted for the second (SeqNo / conflict policy).
    void RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus expectedTx2CommitStatus) {
        CreateTopic("topic_A", TEST_CONSUMER, 1);

        const std::string producer = TEST_MESSAGE_GROUP_ID;

        auto makeSettings = [&]() {
            NTopic::TWriteSessionSettings options;
            options.Path(GetTopicUtPath("topic_A"));
            options.ProducerId(producer);
            options.MessageGroupId(producer);
            options.Codec(ECodec::RAW);
            AugmentWriteSessionSettings(options);
            return options;
        };

        NTopic::TTopicClient client(GetDriver());

        auto session1 = CreateSession();
        auto tx1 = session1->BeginTx();
        {
            auto ws1 = client.CreateSimpleBlockingWriteSession(makeSettings());
            UNIT_ASSERT_C(ws1->Write(NTopic::TWriteMessage("m1"), tx1.get()), "write session 1, message 1");
            UNIT_ASSERT_C(ws1->Write(NTopic::TWriteMessage("m2"), tx1.get()), "write session 1, message 2");
            UNIT_ASSERT_C(ws1->Close(), "close write session 1");
        }

        auto session2 = CreateSession();
        auto tx2 = session2->BeginTx();
        {
            auto ws2 = client.CreateSimpleBlockingWriteSession(makeSettings());
            UNIT_ASSERT_C(ws2->Write(NTopic::TWriteMessage("m3"), tx2.get()), "write session 2, message 1");
            UNIT_ASSERT_C(ws2->Write(NTopic::TWriteMessage("m4"), tx2.get()), "write session 2, message 2");
            UNIT_ASSERT_C(ws2->Close(), "close write session 2");
        }

        session1->CommitTx(*tx1, EStatus::SUCCESS);
        session2->CommitTx(*tx2, expectedTx2CommitStatus);

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        switch (expectedTx2CommitStatus) {
        case EStatus::ABORTED:
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(messages[0], "m1");
            UNIT_ASSERT_VALUES_EQUAL(messages[1], "m2");
            break;
        case EStatus::SUCCESS:
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(messages[0], "m1");
            UNIT_ASSERT_VALUES_EQUAL(messages[1], "m2");
            UNIT_ASSERT_VALUES_EQUAL(messages[2], "m3");
            UNIT_ASSERT_VALUES_EQUAL(messages[3], "m4");
            break;
        default:
            UNIT_FAIL("unexpected expectedTx2CommitStatus for topic read assertions");
            break;
        }
    }
};

template<bool EnableSkipConflictCheckForTopicsInTransaction, ETrackProducerIdInTxMeta TrackProducerIdInTxMeta>
class TFixtureTopicTxMatrix : public TFixtureTopicTxMatrixBase {
protected:
    void AugmentServerSettings(NKikimr::Tests::TServerSettings& settings) override {
        settings.SetEnableSkipConflictCheckForTopicsInTransaction(EnableSkipConflictCheckForTopicsInTransaction);
    }

    void AugmentWriteSessionSettings(NTopic::TWriteSessionSettings& options) override {
        if constexpr (TrackProducerIdInTxMeta == ETrackProducerIdInTxMeta::True) {
            options.AppendSessionMeta(TRACK_PRODUCER_ID_IN_TX_META_KEY, "true");
        } else if constexpr (TrackProducerIdInTxMeta == ETrackProducerIdInTxMeta::False) {
            options.AppendSessionMeta(TRACK_PRODUCER_ID_IN_TX_META_KEY, "false");
        }
    }
};

using TFixture_SkipConflictOff_MetaAbsent = TFixtureTopicTxMatrix<false, ETrackProducerIdInTxMeta::Absent>;
using TFixture_SkipConflictOff_MetaTrue = TFixtureTopicTxMatrix<false, ETrackProducerIdInTxMeta::True>;
using TFixture_SkipConflictOff_MetaFalse = TFixtureTopicTxMatrix<false, ETrackProducerIdInTxMeta::False>;
using TFixture_SkipConflictOn_MetaAbsent = TFixtureTopicTxMatrix<true, ETrackProducerIdInTxMeta::Absent>;
using TFixture_SkipConflictOn_MetaTrue = TFixtureTopicTxMatrix<true, ETrackProducerIdInTxMeta::True>;
using TFixture_SkipConflictOn_MetaFalse = TFixtureTopicTxMatrix<true, ETrackProducerIdInTxMeta::False>;

} // namespace

Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta) {

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOff_MetaAbsent, TFixture_SkipConflictOff_MetaAbsent) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::ABORTED);
}

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOff_MetaTrue, TFixture_SkipConflictOff_MetaTrue) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::ABORTED);
}

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOff_MetaFalse, TFixture_SkipConflictOff_MetaFalse) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::ABORTED);
}

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOn_MetaAbsent, TFixture_SkipConflictOn_MetaAbsent) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::ABORTED);
}

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOn_MetaTrue, TFixture_SkipConflictOn_MetaTrue) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::ABORTED);
}

Y_UNIT_TEST_F(SeqNoConflict_TwoWriteSessions_SkipConflictOn_MetaFalse, TFixture_SkipConflictOn_MetaFalse) {
    RunSeqNoConflictTwoWriteSessionsSameProducer(EStatus::SUCCESS);
}

} // Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta)

} // namespace NYdb::inline Dev::NTopic::NTests::NTxUsage
