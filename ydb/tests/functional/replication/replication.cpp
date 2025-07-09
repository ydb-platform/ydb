#include <ydb/core/transfer/ut/common/utils.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Replication)
{
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
}

