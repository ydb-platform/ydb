#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/txusage_fixture.h>

#include <ydb/library/persqueue/constants.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <optional>
#include <string>
#include <variant>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

namespace {

enum class ETrackProducerIdInTxMeta {
    Absent,
    True,
    False,
};

template <class FixtureBase>
class TFixtureTopicTxMatrixBase : public FixtureBase {
protected:
    using typename FixtureBase::TTableRecord;
    using FixtureBase::AugmentWriteSessionSettings;
    using FixtureBase::CreateSession;
    using FixtureBase::CreateTable;
    using FixtureBase::CreateTopic;
    using FixtureBase::GetDriver;
    using FixtureBase::GetTableRecordsCount;
    using FixtureBase::GetTopicUtPath;
    using FixtureBase::ReadFromTopic;
    using FixtureBase::UpsertToTable;

    size_t CountTableRowsWithKey(const std::string& tablePath, const std::string& key) {
        auto session = CreateSession();
        auto tx = session->BeginTx();
        const auto query = Sprintf(
            R"(DECLARE $k AS Utf8; SELECT COUNT(*) AS cnt FROM `%s` WHERE key = $k;)",
            tablePath.c_str());
        auto params = TParamsBuilder().AddParam("$k").Utf8(key).Build().Build();
        auto result = session->Execute(query, tx.get(), true, params);
        NYdb::TResultSetParser parser(result.at(0));
        UNIT_ASSERT(parser.TryNextRow());
        return static_cast<size_t>(parser.ColumnParser(0).GetUint64());
    }

    void AssertTableKeyValue(const std::string& tablePath, const std::string& key, const std::string& expectedValue) {
        auto session = CreateSession();
        auto tx = session->BeginTx();
        const auto query = Sprintf(
            R"(DECLARE $k AS Utf8; SELECT value FROM `%s` WHERE key = $k;)",
            tablePath.c_str());
        auto params = TParamsBuilder().AddParam("$k").Utf8(key).Build().Build();
        auto result = session->Execute(query, tx.get(), true, params);
        NYdb::TResultSetParser parser(result.at(0));
        UNIT_ASSERT_C(parser.TryNextRow(), key.c_str());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUtf8(), expectedValue);
    }

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

    /// Same as \ref RunSeqNoConflictTwoWriteSessionsSameProducer, but each transaction also upserts disjoint rows
    /// into a row table so KQP uses distributed commit (topic tablet + data shard), not immediate topic-only commit.
    void RunSeqNoConflictTwoWriteSessionsSameProducerDistributed(EStatus expectedTx2CommitStatus) {
        static constexpr const char* kTable = "table_A";
        const std::vector<TTableRecord> tx1Rows = {
            {"dist_seq_tx1_a", "v1"},
            {"dist_seq_tx1_b", "v2"},
        };
        const std::vector<TTableRecord> tx2Rows = {
            {"dist_seq_tx2_a", "v3"},
            {"dist_seq_tx2_b", "v4"},
        };

        CreateTopic("topic_A", TEST_CONSUMER, 1);
        CreateTable("/Root/table_A");

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
        UpsertToTable(kTable, tx1Rows, *session1, tx1.get());

        auto session2 = CreateSession();
        auto tx2 = session2->BeginTx();
        {
            auto ws2 = client.CreateSimpleBlockingWriteSession(makeSettings());
            UNIT_ASSERT_C(ws2->Write(NTopic::TWriteMessage("m3"), tx2.get()), "write session 2, message 1");
            UNIT_ASSERT_C(ws2->Write(NTopic::TWriteMessage("m4"), tx2.get()), "write session 2, message 2");
            UNIT_ASSERT_C(ws2->Close(), "close write session 2");
        }
        UpsertToTable(kTable, tx2Rows, *session2, tx2.get());

        session1->CommitTx(*tx1, EStatus::SUCCESS);
        session2->CommitTx(*tx2, expectedTx2CommitStatus);

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));

        switch (expectedTx2CommitStatus) {
        case EStatus::ABORTED:
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(messages[0], "m1");
            UNIT_ASSERT_VALUES_EQUAL(messages[1], "m2");
            UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount(kTable), 2u);
            for (const auto& row : tx1Rows) {
                UNIT_ASSERT_VALUES_EQUAL(CountTableRowsWithKey(kTable, row.Key), 1u);
                AssertTableKeyValue(kTable, row.Key, row.Value);
            }
            for (const auto& row : tx2Rows) {
                UNIT_ASSERT_VALUES_EQUAL(CountTableRowsWithKey(kTable, row.Key), 0u);
            }
            break;
        case EStatus::SUCCESS:
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(messages[0], "m1");
            UNIT_ASSERT_VALUES_EQUAL(messages[1], "m2");
            UNIT_ASSERT_VALUES_EQUAL(messages[2], "m3");
            UNIT_ASSERT_VALUES_EQUAL(messages[3], "m4");
            UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount(kTable), 4u);
            for (const auto& row : tx1Rows) {
                UNIT_ASSERT_VALUES_EQUAL(CountTableRowsWithKey(kTable, row.Key), 1u);
                AssertTableKeyValue(kTable, row.Key, row.Value);
            }
            for (const auto& row : tx2Rows) {
                UNIT_ASSERT_VALUES_EQUAL(CountTableRowsWithKey(kTable, row.Key), 1u);
                AssertTableKeyValue(kTable, row.Key, row.Value);
            }
            break;
        default:
            UNIT_FAIL("unexpected expectedTx2CommitStatus for distributed topic/table assertions");
            break;
        }
    }
};

template <bool EnableSkipConflictCheckForTopicsInTransaction, ETrackProducerIdInTxMeta TrackProducerIdInTxMeta, class FixtureBase>
class TFixtureTopicTxMatrix : public TFixtureTopicTxMatrixBase<FixtureBase> {
protected:
    void AugmentServerSettings(NKikimr::Tests::TServerSettings& settings) override {
        settings.SetEnableSkipConflictCheckForTopicsInTransaction(EnableSkipConflictCheckForTopicsInTransaction);
    }

    void AugmentWriteSessionSettings(NTopic::TWriteSessionSettings& options) override {
        if ((TrackProducerIdInTxMeta == ETrackProducerIdInTxMeta::True) ||
            (TrackProducerIdInTxMeta == ETrackProducerIdInTxMeta::False)) {
            options.SetTrackProducerIdInTx(TrackProducerIdInTxMeta == ETrackProducerIdInTxMeta::True);
        }
    }
};

#define DEFINE_TOPIC_TX_MATRIX_FIXTURES(SkipBool, MetaEnumVal, Suffix)                                               \
    using TFixture_##Suffix##_Table = TFixtureTopicTxMatrix<(SkipBool), ETrackProducerIdInTxMeta::MetaEnumVal, TFixtureTable>; \
    using TFixture_##Suffix##_Query = TFixtureTopicTxMatrix<(SkipBool), ETrackProducerIdInTxMeta::MetaEnumVal, TFixtureQuery>;

DEFINE_TOPIC_TX_MATRIX_FIXTURES(false, Absent, SkipConflictOff_MetaAbsent)
DEFINE_TOPIC_TX_MATRIX_FIXTURES(false, True, SkipConflictOff_MetaTrue)
DEFINE_TOPIC_TX_MATRIX_FIXTURES(false, False, SkipConflictOff_MetaFalse)
DEFINE_TOPIC_TX_MATRIX_FIXTURES(true, Absent, SkipConflictOn_MetaAbsent)
DEFINE_TOPIC_TX_MATRIX_FIXTURES(true, True, SkipConflictOn_MetaTrue)
DEFINE_TOPIC_TX_MATRIX_FIXTURES(true, False, SkipConflictOn_MetaFalse)

#undef DEFINE_TOPIC_TX_MATRIX_FIXTURES

} // namespace

Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta) {

#define Y_SEQNO_TWO_WRITE_SESSIONS(TestBaseName, FixtureSuffix, ExpectedStatus)                                      \
    Y_UNIT_TEST_F(TestBaseName##_Table_##FixtureSuffix, TFixture_##FixtureSuffix##_Table) {                          \
        RunSeqNoConflictTwoWriteSessionsSameProducer(ExpectedStatus);                                                \
    }                                                                                                                \
    Y_UNIT_TEST_F(TestBaseName##_Query_##FixtureSuffix, TFixture_##FixtureSuffix##_Query) {                          \
        RunSeqNoConflictTwoWriteSessionsSameProducer(ExpectedStatus);                                                \
    }

Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOff_MetaAbsent, EStatus::ABORTED)
Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOff_MetaTrue, EStatus::ABORTED)
Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOff_MetaFalse, EStatus::ABORTED)
Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOn_MetaAbsent, EStatus::ABORTED)
Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOn_MetaTrue, EStatus::ABORTED)
Y_SEQNO_TWO_WRITE_SESSIONS(SeqNoConflict_TwoWriteSessions, SkipConflictOn_MetaFalse, EStatus::SUCCESS)

#undef Y_SEQNO_TWO_WRITE_SESSIONS

#define Y_SEQNO_DISTRIBUTED(TestBaseName, FixtureSuffix, ExpectedStatus)                                             \
    Y_UNIT_TEST_F(TestBaseName##_Table_##FixtureSuffix, TFixture_##FixtureSuffix##_Table) {                          \
        RunSeqNoConflictTwoWriteSessionsSameProducerDistributed(ExpectedStatus);                                     \
    }                                                                                                                \
    Y_UNIT_TEST_F(TestBaseName##_Query_##FixtureSuffix, TFixture_##FixtureSuffix##_Query) {                          \
        RunSeqNoConflictTwoWriteSessionsSameProducerDistributed(ExpectedStatus);                                     \
    }

Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOff_MetaAbsent, EStatus::ABORTED)
Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOff_MetaTrue, EStatus::ABORTED)
Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOff_MetaFalse, EStatus::ABORTED)
Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOn_MetaAbsent, EStatus::ABORTED)
Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOn_MetaTrue, EStatus::ABORTED)
Y_SEQNO_DISTRIBUTED(SeqNoConflict_Distributed_TwoWriteSessions, SkipConflictOn_MetaFalse, EStatus::SUCCESS)

#undef Y_SEQNO_DISTRIBUTED

#define Y_INVALID_WRITE_SESSION_ATTR_TEST(TestBaseName, FixtureType)                                                 \
    Y_UNIT_TEST_F(TestBaseName, FixtureType) {                                                                       \
        CreateTopic("topic_A", TEST_CONSUMER, 1);                                                                  \
                                                                                                                     \
        NTopic::TTopicClient client(GetDriver());                                                                    \
        NTopic::TWriteSessionSettings options;                                                                       \
        options.Path(GetTopicUtPath("topic_A"));                                                                     \
        options.ProducerId(TEST_MESSAGE_GROUP_ID);                                                                   \
        options.MessageGroupId(TEST_MESSAGE_GROUP_ID);                                                               \
        options.Codec(ECodec::RAW);                                                                                  \
        const auto& key = ::NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX;                             \
        options.AppendSessionMeta({key.data(), key.size()}, "not-a-bool");                                           \
                                                                                                                     \
        auto ws = client.CreateWriteSession(options);                                                                \
                                                                                                                     \
        std::optional<NTopic::TSessionClosedEvent> closed;                                                           \
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);                                        \
        while (!closed.has_value()) {                                                                               \
            UNIT_ASSERT_C(                                                                                           \
                TInstant::Now() < deadline,                                                                          \
                "timed out waiting for write session close after invalid WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX"); \
                                                                                                                     \
            if (auto ev = ws->GetEvent(false)) {                                                                     \
                if (auto* c = std::get_if<NTopic::TSessionClosedEvent>(&*ev)) {                                      \
                    closed.emplace(*c);                                                                              \
                }                                                                                                    \
            } else {                                                                                                 \
                Sleep(TDuration::MilliSeconds(50));                                                                  \
            }                                                                                                        \
        }                                                                                                            \
        UNIT_ASSERT_C(closed.has_value(), "session must close after invalid WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX"); \
        UNIT_ASSERT_VALUES_EQUAL(closed->GetStatus(), EStatus::BAD_REQUEST);                                       \
        const TString issues = closed->GetIssues().ToOneLineString();                                              \
        UNIT_ASSERT_STRING_CONTAINS(issues, key);                                                                  \
        UNIT_ASSERT_STRING_CONTAINS(issues, "not-a-bool");                                                           \
                                                                                                                     \
        ws->Close(TDuration::Seconds(5));                                                                          \
    }

Y_INVALID_WRITE_SESSION_ATTR_TEST(InvalidWriteSessionAttributeTrackProducerIdInTx_RejectsInit_Table, TFixtureTable)
Y_INVALID_WRITE_SESSION_ATTR_TEST(InvalidWriteSessionAttributeTrackProducerIdInTx_RejectsInit_Query, TFixtureQuery)

#undef Y_INVALID_WRITE_SESSION_ATTR_TEST

} // Y_UNIT_TEST_SUITE(TopicTxSkipConflictAndProducerMeta)

} // namespace NYdb::inline Dev::NTopic::NTests::NTxUsage
