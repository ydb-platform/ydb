#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/txusage_fixture.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

Y_UNIT_TEST_SUITE(TxUsage) {

Y_UNIT_TEST_F(Transactions_Conflict_On_SeqNo_Table, TFixtureTable)
{
    TestTransactionsConflictOnSeqNo();
}

Y_UNIT_TEST_F(Transactions_Conflict_On_SeqNo_Query, TFixtureQuery)
{
    TestTransactionsConflictOnSeqNo();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_44_Table, TFixtureTable)
{
    TestWriteToTopic44();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_44_Query, TFixtureQuery)
{
    TestWriteToTopic44();
}

Y_UNIT_TEST_F(Write_Random_Sized_Messages_In_Wide_Transactions_Table, TFixtureTable)
{
    TestWriteRandomSizedMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_Random_Sized_Messages_In_Wide_Transactions_Query, TFixtureQuery)
{
    TestWriteRandomSizedMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_Only_Big_Messages_In_Wide_Transactions_Table, TFixtureTable)
{
    TestWriteOnlyBigMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_Only_Big_Messages_In_Wide_Transactions_Query, TFixtureQuery)
{
    TestWriteOnlyBigMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_And_Read_Big_Messages_1, TFixtureNoClient)
{
    TestWriteAndReadMessages(27, 64'000 * 12, false);
}

Y_UNIT_TEST_F(Write_And_Read_Big_Messages_2, TFixtureNoClient)
{
    TestWriteAndReadMessages(27, 64'000 * 12, true);
}

Y_UNIT_TEST_F(Write_And_Read_Huge_Messages_1, TFixtureNoClient)
{
    TestWriteAndReadMessages(4, 9'000'000, false);
}

Y_UNIT_TEST_F(Write_And_Read_Huge_Messages_2, TFixtureNoClient)
{
    TestWriteAndReadMessages(4, 9'000'000, true);
}

Y_UNIT_TEST_F(Write_And_Read_Gigant_Messages_1, TFixtureNoClient)
{
    TestWriteAndReadMessages(4, 61'000'000, false);
}

Y_UNIT_TEST_F(Write_And_Read_Gigant_Messages_2, TFixtureNoClient)
{
    TestWriteAndReadMessages(4, 61'000'000, true);
}

Y_UNIT_TEST_F(Write_50k_100times_50tx, TFixtureTable)
{
    // 100 transactions. Write 100 50KB messages in each folder. Call the commit at the same time.
    // As a result, there will be a lot of small blobs in the FastWrite zone of the main batch,
    // which will be picked up by a compact. The scenario is similar to the work of Ya.Metrika.

    const std::size_t PARTITIONS_COUNT = 2;
    const std::size_t TXS_COUNT = 50;

    auto makeSourceId = [](unsigned txId, unsigned partitionId) {
        std::string sourceId = TEST_MESSAGE_GROUP_ID;
        sourceId += "_";
        sourceId += ToString(txId);
        sourceId += "_";
        sourceId += ToString(partitionId);
        return sourceId;
    };

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        auto sourceId = makeSourceId(i, 0);
        for (size_t j = 0; j < 100; ++j) {
            WriteToTopic("topic_A", sourceId, std::string(50'000, 'x'), tx.get(), 0);
        }
        WaitForAcks("topic_A", sourceId);

        sourceId = makeSourceId(i, 1);
        WriteToTopic("topic_A", sourceId, std::string(50'000, 'x'), tx.get(), 1);
        WaitForAcks("topic_A", sourceId);
    }

    // We are doing an asynchronous commit of transactions. They will be executed simultaneously.
    std::vector<TAsyncStatus> futures;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // All transactions must be completed successfully.
    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

}

}
