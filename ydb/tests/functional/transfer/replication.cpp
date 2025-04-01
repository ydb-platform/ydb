#include "utils.h"

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(Replication)
{
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

        testCase.DropReplicatopn();
    }
}

