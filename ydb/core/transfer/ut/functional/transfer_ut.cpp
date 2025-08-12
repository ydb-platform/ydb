#include <ydb/core/transfer/ut/common/transfer_common.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Transfer)
{
    void CheckTopicLocalOrRemote(bool local) {
        MainTestCase testCase(std::nullopt, "ROW");

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:$x._offset,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(local));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(BaseScenario_Local) {
        CheckTopicLocalOrRemote(true);
    }

    Y_UNIT_TEST(BaseScenario_Remote) {
        CheckTopicLocalOrRemote(false);
    }

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

    Y_UNIT_TEST(ConnectionString_BadChar)
    {
        MainTestCase testCase;

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
                );
            )");
        testCase.CreateTopic(1);
        testCase.ExecuteDDL(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key: 1,
                            Message:CAST("Message-1" AS Utf8)
                        |>
                    ];
                };

                CREATE TRANSFER %s
                FROM %s TO %s USING $l
                WITH (
                    CONNECTION_STRING = "grp§c://localhost:2135"
                )
            )", testCase.TransferName.data(), testCase.TopicName.data(), testCase.TableName.data()));

        testCase.CheckTransferStateError("DNS resolution failed for grp§c://localhost:2135");

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(ConnectionString_BadDNSName)
    {
        MainTestCase testCase;

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
                );
            )");
        testCase.CreateTopic(1);
        testCase.ExecuteDDL(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key: 1,
                            Message:CAST("Message-1" AS Utf8)
                        |>
                    ];
                };

                CREATE TRANSFER %s
                FROM %s TO %s USING $l
                WITH (
                    CONNECTION_STRING = "grpc://domain-not-exists-localhost.com.moc:2135"
                )
            )", testCase.TransferName.data(), testCase.TopicName.data(), testCase.TableName.data()));

        testCase.CheckTransferStateError("Grpc error response on endpoint domain-not-exists-localhost.com.moc:2135");

        testCase.DropTransfer();
        testCase.DropTable();
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
                    STORE = ROW
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
                    STORE = ROW
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

    Y_UNIT_TEST(LocalTopic_WithPermission)
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
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(true));
        
        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTopic();
        testCase.DropTransfer();
    }

    Y_UNIT_TEST(LocalTopic_BigMessage)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Uint32,
                    PRIMARY KEY (Key)
                );
            )");
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:LENGTH($x._data)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(true));

        TStringBuilder sb;
        sb.reserve(10_MB);
        for (size_t i = 0; i < sb.capacity(); ++i) {
            sb << RandomNumber<char>();
        }

        testCase.Write({sb});

        testCase.CheckResult({{
            _C("Message", ui32(sb.size()))
        }});

        testCase.DropTopic();
        testCase.DropTransfer();
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
                    STORE = ROW
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:COALESCE(CAST($x._data || " new lambda" AS Utf8), "Message is empty")
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
                                Message:COALESCE(CAST($x._data || " 1 lambda" AS Utf8), "Message is empty")
                            |>
                        ];
                    };
                )",
                R"(
                    $l = ($x) -> {
                        return [
                            <|
                                Key:CAST($x._offset AS Uint64),
                                Message:COALESCE(CAST($x._data || " 2 lambda" AS Utf8), "Message is empty")
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
                    STORE = ROW
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

    void CheckCommittedOffset(bool local)
    {
        MainTestCase testCase;

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
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
        )", MainTestCase::CreateTransferSettings::WithLocalTopic(local));

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

    Y_UNIT_TEST(CheckCommittedOffset_Local) {
        CheckCommittedOffset(true);
    }

    Y_UNIT_TEST(CheckCommittedOffset_Remote) {
        CheckCommittedOffset(false);
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
                    STORE = ROW
                );
            )");
        testCase.CreateTopic();
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:Unwrap(CAST($x._data AS Utf8), "data is empty")
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
                    STORE = ROW
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
                    STORE = ROW
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

    Y_UNIT_TEST(DescribeTransferWithErrorTopicNotFound)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
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
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(false));

        testCase.CheckTransferStateError("Path not found");

        auto d = testCase.DescribeTransfer();
        UNIT_ASSERT_VALUES_EQUAL(d.GetTransferDescription().GetState(), TTransferDescription::EState::Error);
        UNIT_ASSERT_VALUES_EQUAL(d.GetTransferDescription().GetSrcPath(), TStringBuilder() << "local/" << testCase.TopicName);
        UNIT_ASSERT_VALUES_EQUAL(d.GetTransferDescription().GetDstPath(), TStringBuilder() << "/local/" << testCase.TableName);

        testCase.DropTransfer();
        testCase.DropTable();
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
                    STORE = ROW
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
                    STORE = ROW
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
                    STORE = ROW
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
                    STORE = ROW
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

    void CreateTransferSourceNotExists(bool localTopic)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
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
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(localTopic));

        testCase.CheckTransferStateError("Discovery error:");

        testCase.DropTransfer();
        testCase.DropTable();
    }

    Y_UNIT_TEST(CreateTransferSourceNotExists) {
        CreateTransferSourceNotExists(false);
    }

    Y_UNIT_TEST(CreateTransferSourceNotExists_LocalTopic) {
        CreateTransferSourceNotExists(true);
    }

    void TransferSourceDropped(bool localTopic)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
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
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(localTopic));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.CheckTransferState(TTransferDescription::EState::Running);

        testCase.DropTopic();

        testCase.CheckTransferStateError("Discovery for all topics failed. The last error was: no path '");

        testCase.DropTransfer();
        testCase.DropTable();
    }

    Y_UNIT_TEST(TransferSourceDropped)
    {
        TransferSourceDropped(false);
    }

    Y_UNIT_TEST(TransferSourceDropped_LocalTopic)
    {
        TransferSourceDropped(true);
    }

    void CreateTransferSourceIsNotTopic(bool localTopic)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
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
            )", MainTestCase::CreateTransferSettings::WithLocalTopic(localTopic));

        testCase.CheckTransferStateError("Discovery error:");

        testCase.DropTransfer();
        testCase.DropTable();
    }

    Y_UNIT_TEST(CreateTransferSourceIsNotTopic)
    {
        CreateTransferSourceIsNotTopic(false);
    }

    Y_UNIT_TEST(CreateTransferSourceIsNotTopic_LocalTopic)
    {
        CreateTransferSourceIsNotTopic(true);
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
                    STORE = ROW
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
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Seconds(2), 1_MB));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.CheckTransferState(TTransferDescription::EState::Running);

        Cerr << "State: Paused" << Endl << Flush;

        testCase.PauseTransfer();

        Sleep(TDuration::Seconds(1));
        testCase.CheckTransferState(TTransferDescription::EState::Paused);

        testCase.Write({"Message-2"});

        // Transfer is paused. New messages aren`t added to the table.
        Sleep(TDuration::Seconds(3));
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        Cerr << "State: StandBy" << Endl << Flush;

        testCase.ResumeTransfer();

        // Transfer is resumed. New messages are added to the table.
        testCase.CheckTransferState(TTransferDescription::EState::Running);
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2")),
        }});

        testCase.Write({"Message-3"});
        Sleep(TDuration::MilliSeconds(500));

        // More cycles for pause/resume
        testCase.PauseTransfer();

        testCase.Write({"Message-4"});

        testCase.ResumeTransfer();
        testCase.CheckTransferState(TTransferDescription::EState::Running);

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2")),
        }, {
            _C("Message", TString("Message-3")),
        }, {
            _C("Message", TString("Message-4")),
        }});

        testCase.DropTransfer();
        testCase.DropTable();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(TargetTableWithoutDirectory)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "other_table",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.Write({"Message-1"});
        testCase.CheckTransferStateError("it is not allowed to specify a table to write");

        testCase.DropTransfer();
        testCase.DropTopic();
        testCase.DropTable();
    }

    Y_UNIT_TEST(TargetTableWriteOutsideDirectory)
    {
        MainTestCase testCase;
        testCase.CreateDirectory("/local/inner_directory");

        testCase.CreateTable(R"(
                CREATE TABLE `inner_directory/%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                );
            )");
        testCase.ExecuteDDL(R"(
                CREATE TABLE `outside_directorty_table` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                );
            )");

        testCase.CreateTopic();

        testCase.ExecuteDDL(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "../outside_directorty_table",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };

                CREATE TRANSFER `%s`
                FROM `%s` TO `inner_directory/%s` USING $l
                WITH (
                    DIRECTORY = "/local/inner_directory"
                );
            )", testCase.TransferName.data(), testCase.TopicName.data(), testCase.TableName.data()));

        testCase.Write({"Message-1"});
        testCase.CheckTransferStateError("is outside target directory");

        testCase.DropTransfer();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(TargetTableWriteInsideDirectory)
    {
        MainTestCase testCase;
        testCase.CreateDirectory("/local/inner_directory0");
        testCase.CreateDirectory("/local/inner_directory0/inner_directory1");

        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                );
            )");
        testCase.ExecuteDDL(R"(
                CREATE TABLE `inner_directory0/inner_directory1/table` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                );
            )");

        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "inner_directory1/table",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithDirectory("/local/inner_directory0"));

        testCase.Write({"Message-1"});
        testCase.CheckResult("/local/inner_directory0/inner_directory1/table",{{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTransfer();
        testCase.DropTopic();
    }

    Y_UNIT_TEST(AlterTargetDirectory)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "%s",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", testCase.TableName.data()));

        testCase.Write({"Message-1"});
        testCase.CheckTransferStateError("it is not allowed to specify a table to write");

        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithDirectory("/local"));
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.DropTransfer();
        testCase.DropTopic();
        testCase.DropTable();
    }

    Y_UNIT_TEST(WriteToNotExists)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "not_exists_table",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithDirectory("/local"));

        testCase.Write({"Message-1"});
        testCase.CheckTransferStateError(" unknown table");

        testCase.DropTransfer();
        testCase.DropTopic();
        testCase.DropTable();
    }

    Y_UNIT_TEST(WriteToNotTable)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            __ydb_table: "%s",
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", testCase.TopicName.data()),
            MainTestCase::CreateTransferSettings::WithDirectory("/local"));

        testCase.Write({"Message-1"});
        testCase.CheckTransferStateError(" unknown table");

        testCase.DropTransfer();
        testCase.DropTopic();
        testCase.DropTable();
    }

    Y_UNIT_TEST(AlterLambdaOnWork)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", testCase.TableName.data()));

        testCase.Write({"Message-1"});
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithTransformLambda(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST("NEW LAMBDA " || $x._data AS Utf8)
                        |>
                    ];
                };
            )"));

        testCase.Write({"Message-2"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("NEW LAMBDA Message-2"))
        }});

        testCase.DropTransfer();
        testCase.DropTopic();
        testCase.DropTable();
    }

    Y_UNIT_TEST(CreateAndAlterTransferInDirectory)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = %s
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateDirectory("/local/subdir");
        testCase.CreateTransfer(TStringBuilder() << "subdir/" << testCase.TransferName, Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", testCase.TableName.data()));

        testCase.AlterTransfer(TStringBuilder() << "subdir/" << testCase.TransferName,
            MainTestCase::AlterTransferSettings::WithBatching(TDuration::Seconds(1), 1));
    }

    Y_UNIT_TEST(Alter_WithSecret)
    {
        auto id = RandomNumber<ui16>();
        auto username = TStringBuilder() << "u" << id;
        auto secretName = TStringBuilder() << "s" << id;

        MainTestCase permissionSetup;
        permissionSetup.CreateUser(username);
        permissionSetup.Grant("", username, {"ydb.granular.create_table", "ydb.granular.create_queue", "ydb.granular.alter_schema"});

        MainTestCase testCase(username);
        testCase.ExecuteDDL(Sprintf(R"(
                CREATE OBJECT %s (TYPE SECRET) WITH value="%s@builtin"
            )", secretName.data(), username.data()));

        permissionSetup.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = ROW
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
            )", MainTestCase::CreateTransferSettings::WithSecretName(secretName));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.PauseTransfer();
        testCase.CheckTransferState(TTransferDescription::EState::Paused);
        testCase.ResumeTransfer();

        testCase.Write({"Message-2"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2"))
        }});

        testCase.DropTopic();
        testCase.DropTransfer();
    }

    Y_UNIT_TEST(MessageField_Key) {
        MainTestCase(std::nullopt).Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    Value Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            Value:CAST($x._key AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withAttributes({ {"__key", "key_value"} })},

            .Expectations = {{
                _C("Value", TString("key_value")),
            }}
        });
    }

    Y_UNIT_TEST(MessageField_Key_Empty) {
        MainTestCase(std::nullopt).Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    Value Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = %s
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            Value:CAST($x._key AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withAttributes({ {"__not_key", "key_value"} })},

            .Expectations = {{
                _T<NullChecker>("Value"),
            }}
        });
    }
}

