#include "utils.h"

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(Main_ColumnTable_KeyColumnFirst)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_KeyColumnLast)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Message Utf8 NOT NULL,
                    Key Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_ComplexKey)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key1 Uint64 NOT NULL,
                    Key3 Uint64 NOT NULL,
                    Value1 Utf8,
                    Key2 Uint64 NOT NULL,
                    Value2 Utf8,
                    Key4 Uint64 NOT NULL,
                    PRIMARY KEY (Key3, Key2, Key1, Key4)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key1:CAST(1 AS Uint64),
                            Key2:CAST(2 AS Uint64),
                            Value2:CAST("value-2" AS Utf8),
                            Key4:CAST(4 AS Uint64),
                            Key3:CAST(3 AS Uint64),
                            Value1:CAST("value-1" AS Utf8),
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key1", ui64(1)),
                _C("Key2", ui64(2)),
                _C("Key3", ui64(3)),
                _C("Key4", ui64(4)),
                _C("Value1", TString("value-1")),
                _C("Value2", TString("value-2")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_JsonMessage)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Id Uint64 NOT NULL,
                    FirstName Utf8 NOT NULL,
                    LastName Utf8 NOT NULL,
                    Salary Uint64 NOT NULL,
                    PRIMARY KEY (Id)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    $input = CAST($x._data AS JSON);

                    return [
                        <|
                            Id:        Yson::ConvertToUint64($input.id),
                            FirstName: CAST(Yson::ConvertToString($input.first_name) AS Utf8),
                            LastName:  CAST(Yson::ConvertToString($input.last_name) AS Utf8),
                            Salary:    CAST(Yson::ConvertToString($input.salary) AS UInt64)
                        |>
                    ];
                };
            )",

            .Messages = {{R"({
                "id": 1,
                "first_name": "Vasya",
                "last_name": "Pupkin",
                "salary": "123"
            })"}},

            .Expectations = {{
                _C("Id", ui64(1)),
                _C("FirstName", TString("Vasya")),
                _C("LastName", TString("Pupkin")),
                _C("Salary", ui64(123)),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_NullableColumn)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Date)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Date,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message: CAST($x._data AS Date)
                        |>
                    ];
                };
            )",

            .Messages = {{"2025-02-21"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TInstant::ParseIso8601("2025-02-21")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Double)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Double,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message: CAST($x._data AS Double)
                        |>
                    ];
                };
            )",

            .Messages = {{"1.23"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", 1.23),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Utf8_Long)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_Partition)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Partition Uint32 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Partition)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Partition:CAST($x._partition AS Uint32),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1", 7}},

            .Expectations = {{
                _C("Partition", ui32(7)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_SeqNo)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (SeqNo)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            SeqNo:CAST($x._seq_no AS Uint32),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withSeqNo(13)},

            .Expectations = {{
                _C("SeqNo", ui64(13)),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_ProducerId)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    ProducerId Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            ProducerId:CAST($x._producer_id AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withProducerId("Producer-13")},

            .Expectations = {{
                _C("ProducerId", TString("Producer-13")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_MessageGroupId)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    MessageGroupId Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            MessageGroupId:CAST($x._message_group_id AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withMessageGroupId("MessageGroupId-13")},

            .Expectations = {{
                _C("MessageGroupId", TString("MessageGroupId-13")),
            }}
        });
    }

    Y_UNIT_TEST(AlterLambda)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",
    
            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data || " new lambda" AS Utf8)
                        |>
                    ];
                };
            )",
    
            .Messages = {{"Message-1"}},
    
            .Expectations = {{
                _C("Message", TString("Message-1 new lambda")),
            }},

            .AlterLambdas = {
                R"(
                    $l = ($x) -> {
                        return [
                            <|
                                Key:CAST($x._offset AS Uint64),
                                Message:CAST($x._data || " 1 lambda" AS Utf8)
                            |>
                        ];
                    };
                )",
                R"(
                    $l = ($x) -> {
                        return [
                            <|
                                Key:CAST($x._offset AS Uint64),
                                Message:CAST($x._data || " 2 lambda" AS Utf8)
                            |>
                        ];
                    };
                )",
            }
    
        });
    }

    Y_UNIT_TEST(DropTransfer)
    {
        MainTestCase testCase;

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        testCase.CreateTopic();
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.Write({"Message-1"});
        testCase.CheckResult({{
            _C("Key", ui64(0)),
            _C("Message", TString("Message-1")),
        }});

        {
            auto result = testCase.DescribeTransfer();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        testCase.DropTransfer();

        {
            auto result = testCase.DescribeTransfer();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(EStatus::SCHEME_ERROR, result.GetStatus());
        }

        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CreateAndDropConsumer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic();
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        for (size_t i = 20; i--; ) {
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            if (1 == consumers.size()) {
                UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
                Cerr << "Consumer name is '" << consumers[0].GetConsumerName() << "'" << Endl << Flush;
                UNIT_ASSERT_C("replicationConsumer" != consumers[0].GetConsumerName(), "Consumer name is random uuid");
                break;
            }

            UNIT_ASSERT_C(i, "Unable to wait consumer has been created");
            Sleep(TDuration::Seconds(1));
        }

        testCase.DropTransfer();

        for (size_t i = 20; i--; ) {
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            if (0 == consumers.size()) {
                UNIT_ASSERT_VALUES_EQUAL(0, consumers.size());
                break;
            }

            UNIT_ASSERT_C(i, "Unable to wait consumer has been removed");
            Sleep(TDuration::Seconds(1));
        }

        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(DescribeError_OnLambdaCompilation)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return $x._unknown_field_for_lambda_compilation_error;
                };
            )");
        
        testCase.CheckTransferStateError("_unknown_field_for_lambda_compilation_error");

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }
/*
    Y_UNIT_TEST(DescribeError_OnWriteToShard)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:null,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");
        
        testCase.Write({"message-1"});

        testCase.CheckTransferStateError("Cannot write data into shard");
    }
*/

    Y_UNIT_TEST(CustomConsumer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateConsumer("PredefinedConsumer");
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithConsumerName("PredefinedConsumer"));

        Sleep(TDuration::Seconds(3));

        { // Check that consumer is reused
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
            UNIT_ASSERT_VALUES_EQUAL("PredefinedConsumer", consumers[0].GetConsumerName());
        }

        testCase.DropTransfer();

        Sleep(TDuration::Seconds(3));

        { // Check that consumer is not removed
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
            UNIT_ASSERT_VALUES_EQUAL("PredefinedConsumer", consumers[0].GetConsumerName());
        }

        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CustomFlushInterval)
    {
        TDuration flushInterval = TDuration::Seconds(5);

        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(flushInterval, 13_MB));


        TInstant expectedEnd = TInstant::Now() + flushInterval;
        testCase.Write({"Message-1"});

        // check that data in the table only after flush interval
        for (size_t attempt = 20; attempt--; ) {
            auto res = testCase.DoRead({{
                _C("Key", ui64(0)),
            }});
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            if (res.first == 1) {
                UNIT_ASSERT_C(expectedEnd <= TInstant::Now(), "Expected: " << expectedEnd << " Now: " << TInstant::Now());
                break;
            }

            UNIT_ASSERT_C(attempt, "Unable to wait transfer result");
            Sleep(TDuration::Seconds(1));
        }

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(AlterFlushInterval)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Hours(1), 1_GB));


        testCase.Write({"Message-1"});

        // check if there isn`t data in the table (flush_interval is big)
        for (size_t attempt = 5; attempt--; ) {
            auto res = testCase.DoRead({{
                _C("Key", ui64(0)),
            }});
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL_C(0, res.first, "Flush has not been happened");
            Sleep(TDuration::Seconds(1));
        }

        // flush interval is small
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::MilliSeconds(1), 1_GB), false);
        // flush interval is big
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Days(1) + TDuration::Seconds(1), 1_GB), false);

        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Seconds(1), 1_GB));

        // check if there is data in the table
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(AlterBatchSize)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Hours(1), 512_MB));


        testCase.Write({"Message-1"});

        // batch size is big. alter is not success
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1_GB + 1), false);

        // batch size is top valid value. alter is success
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1_GB));

        // batch size is small. alter is success. after flush will
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1));

        // check if there is data in the table
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CreateTransferSourceNotExists)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Discovery error: local/Topic_");

        testCase.DropTransfer();
        testCase.DropTable();
    }

    Y_UNIT_TEST(CreateTransferSourceIsNotTopic)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )", testCase.TopicName.data()));

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Discovery error: local/Topic_");

        testCase.DropTransfer();
        testCase.DropTable();
    }

    Y_UNIT_TEST(CreateTransferRowTable)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Only column tables are supported as transfer targets");

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CreateTransferTargetIsNotTable)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TOPIC `%s`;
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Only column tables are supported as transfer targets");

        testCase.DropTransfer();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CreateTransferTargetNotExists)
    {
        MainTestCase testCase;
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError(TStringBuilder() << "The target table `/local/" << testCase.TableName << "` does not exist");

        testCase.DropTransfer();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(PauseAndResumeTransfer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Seconds(1), 1));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.CheckTransferState(TReplicationDescription::EState::Running);

        Cerr << "State: Paused" << Endl << Flush;

        testCase.PauseTransfer();

        Sleep(TDuration::Seconds(1));
        testCase.CheckTransferState(TReplicationDescription::EState::Paused);

        testCase.Write({"Message-2"});

        // Transfer is paused. New messages aren`t added to the table.
        Sleep(TDuration::Seconds(3));
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        Cerr << "State: StandBy" << Endl << Flush;

        testCase.ResumeTransfer();

        // Transfer is resumed. New messages are added to the table.
        testCase.CheckTransferState(TReplicationDescription::EState::Running);
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2")),
        }});

        // More cycles for pause/resume
        testCase.PauseTransfer();
        testCase.CheckTransferState(TReplicationDescription::EState::Paused);

        testCase.ResumeTransfer();
        testCase.CheckTransferState(TReplicationDescription::EState::Running);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }
}

