#include <ydb/core/transfer/ut/common/utils.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Replication)
{
    void LiteralDefaults(const TString& consistency)
    {
        MainTestCase testCase;
        testCase.CreateSourceTable(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                count Uint64 NOT NULL DEFAULT 0,
                message Utf8 DEFAULT "default message",
                PRIMARY KEY (Key)
            );
        )");
        testCase.ExecuteSourceTableQuery(R"(
            INSERT INTO `%s` (Key, count, message) VALUES (1ul, 7ul, "explicit");
        )");
        testCase.ExecuteSourceTableQuery("INSERT INTO `%s` (Key) VALUES (2ul);");
        testCase.ExecuteDDL(Sprintf(R"(
            CREATE ASYNC REPLICATION `%s` FOR `%s` AS `%s`
            WITH (CONNECTION_STRING = "grpc://%s", CONSISTENCY_LEVEL = "%s");
        )", testCase.ReplicationName.data(), testCase.SourceTableName.data(), testCase.TableName.data(),
                testCase.ConnectionString.data(), consistency.data()));

        TExpectations expected = {
            {_C("Key", ui64(1)), _C("count", ui64(7)), _C("message", TString("explicit"))},
            {_C("Key", ui64(2)), _C("count", ui64(0)), _C("message", TString("default message"))},
        };
        testCase.CheckResult(expected);

        // Check defaults on the live source as well as rows from the initial scan.
        testCase.ExecuteSourceTableQuery("INSERT INTO `%s` (Key) VALUES (3ul);");
        expected.push_back({_C("Key", ui64(3)), _C("count", ui64(0)), _C("message", TString("default message"))});
        testCase.CheckResult(expected);

        testCase.ExecuteDDL(Sprintf(R"(
            ALTER ASYNC REPLICATION `%s` SET (STATE = "DONE");
        )", testCase.ReplicationName.data()));
        testCase.CheckReplicationState(TReplicationDescription::EState::Done);

        for (const auto& [operation, key] : {std::pair{"INSERT", ui64(4)}, std::pair{"UPSERT", ui64(5)}}) {
            const TString query = TStringBuilder() << operation << " INTO `" << testCase.TableName
                                                   << "` (Key) VALUES (" << key << "ul);";
            const auto result = testCase.ExecuteQuery(query, false);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            expected.push_back({_C("Key", ui64(key)), _C("count", ui64(0)), _C("message", TString("default message"))});
        }
        testCase.CheckResult(expected);

        const TString nullQuery = TStringBuilder() << "INSERT INTO `" << testCase.TableName
                                                   << "` (Key, count, message) VALUES (6ul, 99ul, NULL);";
        const auto nullResult = testCase.ExecuteQuery(nullQuery, false);
        UNIT_ASSERT_C(nullResult.IsSuccess(), nullResult.GetIssues().ToString());
        expected.push_back({_C("Key", ui64(6)), _C("count", ui64(99)), _T<NullChecker>("message")});
        testCase.CheckResult(expected);

        testCase.DropReplication();
        testCase.DropSourceTable();
        testCase.ExecuteDDL(Sprintf("DROP TABLE `%s`;", testCase.TableName.data()));
    }

    Y_UNIT_TEST(LiteralDefaultsRow)
    {
        LiteralDefaults("ROW");
    }

    Y_UNIT_TEST(LiteralDefaultsGlobal)
    {
        LiteralDefaults("GLOBAL");
    }

    Y_UNIT_TEST(Types)
    {
        MainTestCase testCase;
        testCase.CreateSourceTable(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Key2 Uuid,
                    v01 Uuid,
                    v02 Uuid NOT NULL,
                    v03 Double,
                    PRIMARY KEY (Key, Key2)
                );
        )");

        testCase.ExecuteSourceTableQuery(R"(
            UPSERT INTO `%s` (Key,Key2,v01,v02,v03) VALUES
            (
                1,
                CAST("00078af5-0000-0000-6c0b-040000000000" as Uuid),
                CAST("00078af5-0000-0000-6c0b-040000000001" as Uuid),
                UNWRAP(CAST("00078af5-0000-0000-6c0b-040000000002" as Uuid)),
                CAST("311111111113.222222223" as Double)
            );
        )");

        testCase.CreateReplication();

        testCase.CheckResult({{
            _C("Key2", TUuidValue("00078af5-0000-0000-6c0b-040000000000")),
            _C("v01", TUuidValue("00078af5-0000-0000-6c0b-040000000001")),
            _C("v02", TUuidValue("00078af5-0000-0000-6c0b-040000000002")),
            _C("v03", 311111111113.222222223)
        }});

        testCase.DropReplication();
        testCase.DropSourceTable();
    }

    Y_UNIT_TEST(PauseAndResumeReplication)
    {
        MainTestCase testCase;
        testCase.CreateSourceTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                );
            )");

        testCase.CreateReplication();

        testCase.ExecuteSourceTableQuery("INSERT INTO `%s` (`Key`, `Message`) VALUES (1, 'Message-1');");

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.CheckReplicationState(TReplicationDescription::EState::Running);

        Cerr << "State: Paused" << Endl << Flush;

        testCase.PauseReplication();

        Sleep(TDuration::Seconds(1));
        testCase.CheckReplicationState(TReplicationDescription::EState::Paused);

        testCase.ExecuteSourceTableQuery("INSERT INTO `%s` (`Key`, `Message`) VALUES (2, 'Message-2');");

        // Replication is paused. New messages aren`t added to the table.
        Sleep(TDuration::Seconds(3));
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        Cerr << "State: StandBy" << Endl << Flush;

        testCase.ResumeReplication();

        // Replication is resumed. New messages are added to the table.
        testCase.CheckReplicationState(TReplicationDescription::EState::Running);
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2")),
        }});

        // More cycles for pause/resume
        testCase.PauseReplication();
        testCase.CheckReplicationState(TReplicationDescription::EState::Paused);

        testCase.ResumeReplication();
        testCase.CheckReplicationState(TReplicationDescription::EState::Running);

        testCase.DropReplication();
        testCase.DropSourceTable();
    }

    Y_UNIT_TEST(TopicAutopartitioning)
    {
        MainTestCase testCase;

        testCase.CreateSourceTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                ) WITH (
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1,
                    AUTO_PARTITIONING_BY_LOAD = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 10,
                    UNIFORM_PARTITIONS = 1
                );
            )");

        testCase.CreateReplication();

        Cerr << "Insert much data to trigger split" << Endl << Flush;
        for (size_t batch = 0; batch < 50; ++batch) {
            TStringBuilder query;
            query << "UPSERT INTO `%s` (Key, Message) VALUES ";
            for (size_t i = 0; i < 100; ++i) {
                if (i > 0) query << ", ";
                size_t key = batch * 100 + i;
                query << "(" << key << ", 'Message-" << TString(10000, 'a') << "')";
            }
            query << ";";
            testCase.ExecuteSourceTableQuery(query);
        }

        Cerr << "Wait for partition split and data replication" << Endl << Flush;
        Sleep(TDuration::Seconds(10));

        Cerr << "Verify data replicated from all partitions" << Endl << Flush;
        TExpectations expectations;
        for (size_t batch = 0; batch < 50; ++batch) {
            for (size_t i = 0; i < 100; ++i) {
                TString msg = TStringBuilder() << "Message-" << TString(10000, 'a');
                expectations.push_back({_C("Message", std::move(msg))});
            }
        }
        testCase.CheckResult(expectations);

        testCase.DropReplication();
        testCase.DropSourceTable();
    }
}

