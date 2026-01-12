#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/txusage_fixture.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

Y_UNIT_TEST_SUITE(TxUsage) {

Y_UNIT_TEST_F(WriteToTopic_Demo_11_Table, TFixtureTable)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_11_Query, TFixtureQuery)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_12_Table, TFixtureTable)
{
    TestWriteToTopic12();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_12_Query, TFixtureQuery)
{
    TestWriteToTopic12();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_13_Table, TFixtureTable)
{
    TestWriteToTopic13();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_13_Query, TFixtureQuery)
{
    TestWriteToTopic13();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_14_Table, TFixtureTable)
{
    TestWriteToTopic14();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_14_Query, TFixtureQuery)
{
    TestWriteToTopic14();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_16_Table, TFixtureTable)
{
    TestWriteToTopic16();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_16_Query, TFixtureQuery)
{
    TestWriteToTopic16();
}

#define Y_UNIT_TEST_WITH_REBOOTS(name, oldHeadCount, bigBlobsCount, newHeadCount) \
Y_UNIT_TEST_F(name##_RestartNo_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartNo}); \
} \
Y_UNIT_TEST_F(name##_RestartNo_Query, TFixtureQuery) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartNo}); \
} \
Y_UNIT_TEST_F(name##_RestartBeforeCommit_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartBeforeCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartBeforeCommit_Query, TFixtureQuery) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartBeforeCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartAfterCommit_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartAfterCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartAfterCommit_Query, TFixtureQuery) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartAfterCommit}); \
}

Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_18, 10, 2, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_19, 10, 0, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_20, 10, 2,  0);

Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_21,  0, 2, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_22,  0, 0, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_23,  0, 2,  0);

Y_UNIT_TEST_F(The_TxWriteInfo_Is_Deleted_After_The_Immediate_Transaction, TFixtureTable)
{
    CreateTopic("topic_A");

    auto session = CreateSession();

    auto tx = session->BeginTx();
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    RestartPQTablet("topic_A", 0);
    session->CommitTx(*tx, EStatus::SUCCESS);

    tx = session->BeginTx();
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    RestartPQTablet("topic_A", 0);
    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");

    CheckTabletKeys("topic_A");

    WaitForTheTabletToDeleteTheWriteInfo("topic_A", 0);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_24_Table, TFixtureTable)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_24_Query, TFixtureQuery)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_27_Table, TFixtureTable)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_27_Query, TFixtureQuery)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_38_Table, TFixtureTable)
{
    TestWriteToTopic38();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_38_Query, TFixtureQuery)
{
    TestWriteToTopic38();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_40_Table, TFixtureTable)
{
    TestWriteToTopic40();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_40_Query, TFixtureQuery)
{
    TestWriteToTopic40();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_41_Table, TFixtureTable)
{
    TestWriteToTopic41();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_41_Query, TFixtureQuery)
{
    TestWriteToTopic41();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_42_Table, TFixtureTable)
{
    TestWriteToTopic42();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_42_Query, TFixtureQuery)
{
    TestWriteToTopic42();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_43_Table, TFixtureTable)
{
    TestWriteToTopic43();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_43_Query, TFixtureQuery)
{
    TestWriteToTopic43();
}

Y_UNIT_TEST_F(ReadRuleGeneration, TFixtureNoClient)
{
    // There was a server
    NotifySchemeShard({.EnablePQConfigTransactionsAtSchemeShard = false});

    // Users have created their own topic on it
    CreateTopic(TEST_TOPIC);

    // And they wrote their messages into it
    WriteToTopic(TEST_TOPIC, TEST_MESSAGE_GROUP_ID, "message-1");
    WriteToTopic(TEST_TOPIC, TEST_MESSAGE_GROUP_ID, "message-2");
    WriteToTopic(TEST_TOPIC, TEST_MESSAGE_GROUP_ID, "message-3");

    // And he had a consumer
    AddConsumer(TEST_TOPIC, {"consumer-1"});

    // We read messages from the topic and committed offsets
    Read_Exactly_N_Messages_From_Topic(TEST_TOPIC, "consumer-1", 3);
    CloseTopicReadSession(TEST_TOPIC, "consumer-1");

    // And then the Logbroker team turned on the feature flag
    NotifySchemeShard({.EnablePQConfigTransactionsAtSchemeShard = true});

    // Users continued to write to the topic
    WriteToTopic(TEST_TOPIC, TEST_MESSAGE_GROUP_ID, "message-4");

    // Users have added new consumers
    AddConsumer(TEST_TOPIC, {"consumer-2"});

    // And they wanted to continue reading their messages
    Read_Exactly_N_Messages_From_Topic(TEST_TOPIC, "consumer-1", 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_45_Table, TFixtureTable)
{
    TestWriteToTopic45();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_45_Query, TFixtureQuery)
{
    TestWriteToTopic45();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_46_Table, TFixtureTable)
{
    TestWriteToTopic46();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_46_Query, TFixtureQuery)
{
    TestWriteToTopic46();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_47_Table, TFixtureTable)
{
    TestWriteToTopic47();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_47_Query, TFixtureQuery)
{
    TestWriteToTopic47();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic7();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic7();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_2_Table, TFixtureSinksTable)
{
    TestWriteToTopic10();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_2_Query, TFixtureSinksQuery)
{
    TestWriteToTopic10();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_3_Table, TFixtureSinksTable)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_3_Query, TFixtureSinksQuery)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_4_Table, TFixtureSinksTable)
{
    TestWriteToTopic9();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_4_Query, TFixtureSinksQuery)
{
    TestWriteToTopic9();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_5_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopic5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_5_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopic5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic1();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic1();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_2_Table, TFixtureSinksTable)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_2_Query, TFixtureSinksQuery)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_3_Table, TFixtureSinksTable)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_3_Query, TFixtureSinksQuery)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_4_Table, TFixtureSinksTable)
{
    TestWriteToTopic4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_4_Query, TFixtureSinksQuery)
{
    TestWriteToTopic4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_2_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_2_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_3_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_3_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_4_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_4_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_5_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_5_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_6_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable6();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_6_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable6();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_1_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable1();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_1_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable1();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_2_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_2_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_3_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_3_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_4_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_4_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(The_Transaction_Starts_On_One_Version_And_Ends_On_The_Other, TFixtureNoClient)
{
    // In the test, we check the compatibility between versions `24-4-2` and `24-4-*/25-1-*`. To do this, the data
    // obtained on the `24-4-2` version is loaded into the PQ tablets.

    CreateTopic("topic_A", TEST_CONSUMER, 2);

    PQTabletPrepareFromResource("topic_A", 0, "topic_A_partition_0_v24-4-2.dat");
    PQTabletPrepareFromResource("topic_A", 1, "topic_A_partition_1_v24-4-2.dat");

    RestartPQTablet("topic_A", 0);
    RestartPQTablet("topic_A", 1);
}

Y_UNIT_TEST_F(Write_And_Read_Small_Messages_1, TFixtureNoClient)
{
    TestWriteAndReadMessages(320, 64'000, false);
}

Y_UNIT_TEST_F(Write_And_Read_Small_Messages_2, TFixtureNoClient)
{
    TestWriteAndReadMessages(320, 64'000, true);
}

}

}
