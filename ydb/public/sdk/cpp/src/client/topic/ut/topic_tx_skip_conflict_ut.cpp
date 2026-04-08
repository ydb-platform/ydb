#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/txusage_fixture.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

namespace {

constexpr const char* TRACK_PRODUCER_ID_IN_TX_META_KEY = "track_producer_id_in_tx";

class TFixtureTopicTxMatrixBase : public TFixtureTable {
protected:
    void RunTopicOnlyInTransaction() {
        CreateTopic("topic_A");

        auto session = CreateSession();
        auto tx = session->BeginTx();
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message", tx.get());
        session->CommitTx(*tx, EStatus::SUCCESS);

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message");
        CheckTabletKeys("topic_A");
    }

    void RunTopicAndTableInTransaction() {
        CreateTopic("topic_A");
        CreateTable("/Root/table_A");

        auto session = CreateSession();
        auto tx = session->BeginTx();

        auto records = MakeTableRecords();
        UpsertToTable("table_A", records, *session, tx.get());
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());
        session->CommitTx(*tx, EStatus::SUCCESS);

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], MakeJsonDoc(records));
        UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
        CheckTabletKeys("topic_A");
    }
};

template<bool EnableSkipConflictCheckForTopicsInTransaction, bool AppendTrackProducerIdInTxMetaTrue>
class TFixtureTopicTxMatrix : public TFixtureTopicTxMatrixBase {
protected:
    void AugmentServerSettings(NKikimr::Tests::TServerSettings& settings) override {
        settings.SetEnableSkipConflictCheckForTopicsInTransaction(EnableSkipConflictCheckForTopicsInTransaction);
    }

    void AugmentWriteSessionSettings(NTopic::TWriteSessionSettings& options) override {
        if constexpr (AppendTrackProducerIdInTxMetaTrue) {
            options.AppendSessionMeta(TRACK_PRODUCER_ID_IN_TX_META_KEY, "true");
        }
    }
};

using TFixture_SkipConflictOff_MetaAbsent = TFixtureTopicTxMatrix<false, false>;
using TFixture_SkipConflictOff_MetaTrue = TFixtureTopicTxMatrix<false, true>;
using TFixture_SkipConflictOn_MetaAbsent = TFixtureTopicTxMatrix<true, false>;
using TFixture_SkipConflictOn_MetaTrue = TFixtureTopicTxMatrix<true, true>;

} // namespace

Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta) {

Y_UNIT_TEST_F(TopicOnly_SkipConflictOff_MetaAbsent, TFixture_SkipConflictOff_MetaAbsent) {
    RunTopicOnlyInTransaction();
}

Y_UNIT_TEST_F(TopicOnly_SkipConflictOff_MetaTrue, TFixture_SkipConflictOff_MetaTrue) {
    RunTopicOnlyInTransaction();
}

Y_UNIT_TEST_F(TopicOnly_SkipConflictOn_MetaAbsent, TFixture_SkipConflictOn_MetaAbsent) {
    RunTopicOnlyInTransaction();
}

Y_UNIT_TEST_F(TopicOnly_SkipConflictOn_MetaTrue, TFixture_SkipConflictOn_MetaTrue) {
    RunTopicOnlyInTransaction();
}

Y_UNIT_TEST_F(TopicAndTable_SkipConflictOff_MetaAbsent, TFixture_SkipConflictOff_MetaAbsent) {
    RunTopicAndTableInTransaction();
}

Y_UNIT_TEST_F(TopicAndTable_SkipConflictOff_MetaTrue, TFixture_SkipConflictOff_MetaTrue) {
    RunTopicAndTableInTransaction();
}

Y_UNIT_TEST_F(TopicAndTable_SkipConflictOn_MetaAbsent, TFixture_SkipConflictOn_MetaAbsent) {
    RunTopicAndTableInTransaction();
}

Y_UNIT_TEST_F(TopicAndTable_SkipConflictOn_MetaTrue, TFixture_SkipConflictOn_MetaTrue) {
    RunTopicAndTableInTransaction();
}

} // Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta)

} // namespace NYdb::inline Dev::NTopic::NTests::NTxUsage
