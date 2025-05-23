#include "transfer_common.h"

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(CreateTransfer_TargetNotFound)
    {
        MainTestCase testCase;
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
            $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64)
                        |>
                    ];
                };
        )", MainTestCase::CreateTransferSettings::WithExpectedError(TStringBuilder() << "Path does not exist"));

        testCase.DropTopic();
    }

    Y_UNIT_TEST(Create_WithPermission)
    {
        auto id = RandomNumber<ui16>();
        auto username = TStringBuilder() << "u" << id;

        MainTestCase permissionSetup;
        permissionSetup.CreateUser(username);
        permissionSetup.Grant("", username, {"ydb.granular.create_table", "ydb.granular.create_queue"});

        MainTestCase testCase(username);
        permissionSetup.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )", testCase.TableName.data()));
        permissionSetup.Grant(testCase.TableName, username, {"ydb.generic.write", "ydb.generic.read"});

        testCase.CreateTopic(1);
        permissionSetup.Grant(testCase.TopicName, username, {"ALL"});

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

        testCase.DropTopic();
        testCase.DropTransfer();
    }

    Y_UNIT_TEST(Create_WithoutTablePermission)
    {
        auto id = RandomNumber<ui16>();
        auto username = TStringBuilder() << "u" << id;

        MainTestCase permissionSetup;
        permissionSetup.CreateUser(username);
        permissionSetup.Grant("", username, {"ydb.granular.create_table", "ydb.granular.create_queue"});

        MainTestCase testCase(username);
        permissionSetup.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )", testCase.TableName.data()));
        permissionSetup.Grant(testCase.TableName, username, {"ydb.generic.read"});

        testCase.CreateTopic(1);
        permissionSetup.Grant(testCase.TopicName, username, {"ALL"});

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithExpectedError("Access denied for scheme request"));

        testCase.DropTopic();
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

    Y_UNIT_TEST(EnsureError)
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
                            Key:Ensure($x._offset, false, "This message will be displayed as an error"),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.Write({"Message-1"});

        testCase.CheckTransferStateError("Condition violated:  This message will be displayed as an error");
        testCase.CheckCommittedOffset(0, 0);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(CheckCommittedOffset)
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
        )");

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Key", ui64(0)),
            _C("Message", TString{"Message-1"}),
        }});
        testCase.CheckCommittedOffset(0, 1);

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
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

    Y_UNIT_TEST(TransferSourceDropped)
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
            )");

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTopic();

        testCase.CheckTransferStateError("Discovery for all topics failed. The last error was: no path 'local/Topic_");

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
            )", MainTestCase::CreateTransferSettings::WithExpectedError(TStringBuilder() << "The transfer destination path '/local/" << testCase.TableName << "' isn`t a table"));

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
            )", MainTestCase::CreateTransferSettings::WithExpectedError("Path does not exist"));

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

