#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/strip.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapJson) {
    class ICommand {
    private:
        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) = 0;

    public:
        virtual ~ICommand() = default;

        TConclusionStatus Execute(TKikimrRunner& kikimr) {
            return DoExecute(kikimr);
        }
    };

    class TSchemaCommand: public ICommand {
    private:
        const TString Command;
        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            Cerr << "EXECUTE: " << Command << Endl;
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(Command).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            return TConclusionStatus::Success();
        }

    public:
        TSchemaCommand(const TString& command)
            : Command(command) {
        }
    };

    class TDataCommand: public ICommand {
    private:
        const TString Command;
        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            Cerr << "EXECUTE: " << Command << Endl;
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto client = kikimr.GetQueryClient();
            auto prepareResult = client.ExecuteQuery(Command, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
            return TConclusionStatus::Success();
        }

    public:
        TDataCommand(const TString& command)
            : Command(command) {
        }
    };

    class TSelectCommand: public ICommand {
    private:
        const TString Command;
        const TString Compare;
        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            Cerr << "EXECUTE: " << Command << Endl;
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto it = kikimr.GetQueryClient().StreamExecuteQuery(Command, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            if (Compare) {
                Cerr << "COMPARE: " << Compare << Endl;
                Cerr << "OUTPUT: " << output << Endl;
                CompareYson(output, Compare);
            }
            return TConclusionStatus::Success();
        }

    public:
        TSelectCommand(const TString& command, const TString& compare)
            : Command(command)
            , Compare(compare) {
        }
    };

    class TStopCompactionCommand: public ICommand {
    private:
        virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override {
            auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
            AFL_VERIFY(controller);
            controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            return TConclusionStatus::Success();
        }

    public:
        TStopCompactionCommand() {
        }
    };

    class TOneCompactionCommand: public ICommand {
    private:
        virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override {
            auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
            AFL_VERIFY(controller);
            AFL_VERIFY(!controller->IsBackgroundEnable(NKikimr::NYDBTest::ICSController::EBackground::Compaction));
            const i64 compactions = controller->GetCompactionFinishedCounter().Val();
            controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            const TInstant start = TInstant::Now();
            while (TInstant::Now() - start < TDuration::Seconds(5)) {
                if (compactions < controller->GetCompactionFinishedCounter().Val()) {
                    Cerr << "COMPACTION_HAPPENED: " << compactions << " -> " << controller->GetCompactionFinishedCounter().Val() << Endl;
                    break;
                }
                Cerr << "WAIT_COMPACTION: " << controller->GetCompactionFinishedCounter().Val() << Endl;
                Sleep(TDuration::MilliSeconds(300));
            }
            AFL_VERIFY(compactions < controller->GetCompactionFinishedCounter().Val());
            controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
            return TConclusionStatus::Success();
        }

    public:
        TOneCompactionCommand() {
        }
    };

    class TWaitCompactionCommand: public ICommand {
    private:
        virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override {
            auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
            AFL_VERIFY(controller);
            controller->WaitCompactions(TDuration::Seconds(5));
            return TConclusionStatus::Success();
        }

    public:
        TWaitCompactionCommand() {
        }
    };

    class TScriptExecutor {
    private:
        std::vector<std::shared_ptr<ICommand>> Commands;

    public:
        TScriptExecutor(const std::vector<std::shared_ptr<ICommand>>& commands)
            : Commands(commands)
        {

        }
        void Execute() {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
            auto settings = TKikimrSettings().SetAppConfig(appConfig).SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
            csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
            csController->SetOverrideMemoryLimitForPortionReading(1e+10);
            csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
            for (auto&& i : Commands) {
                i->Execute(kikimr);
            }
        }
    };

    class TScriptVariator {
    private:
        std::vector<TScriptExecutor> Scripts;
        std::shared_ptr<ICommand> BuildCommand(TString command) {
            if (command.StartsWith("SCHEMA:")) {
                command = command.substr(7);
                return std::make_shared<TSchemaCommand>(command);
            } else if (command.StartsWith("DATA:")) {
                command = command.substr(5);
                return std::make_shared<TDataCommand>(command);
            } else if (command.StartsWith("READ:")) {
                auto lines = StringSplitter(command.substr(5)).SplitBySet("\n").ToList<TString>();
                int step = 0;
                TString request;
                TString expectation;
                for (auto&& i : lines) {
                    i = Strip(i);
                    if (i.StartsWith("EXPECTED:")) {
                        step = 1;
                        i = i.substr(9);
                    }
                    if (step == 0) {
                        request += i;
                    } else if (step == 1) {
                        expectation += i;
                    }
                }
                return std::make_shared<TSelectCommand>(request, expectation);
            } else if (command.StartsWith("WAIT_COMPACTION")) {
                return std::make_shared<TWaitCompactionCommand>();
            } else if (command.StartsWith("STOP_COMPACTION")) {
                return std::make_shared<TStopCompactionCommand>();
            } else if (command.StartsWith("ONE_COMPACTION")) {
                return std::make_shared<TOneCompactionCommand>();
            } else {
                AFL_VERIFY(false)("command", command);
                return nullptr;
            }
        }
        void BuildScripts(const std::vector<std::vector<std::shared_ptr<ICommand>>>& commands, const ui32 currentLayer,
            std::vector<std::shared_ptr<ICommand>>& currentScript, std::vector<TScriptExecutor>& scripts) {
            if (currentLayer == commands.size()) {
                scripts.emplace_back(currentScript);
                return;
            }
            for (auto&& i : commands[currentLayer]) {
                currentScript.emplace_back(i);
                BuildScripts(commands, currentLayer + 1, currentScript, scripts);
                currentScript.pop_back();
            }
        }

        void BuildVariantsImpl(const std::vector<std::vector<TString>>& chunks, const ui32 currentLayer, std::vector<TString>& currentCommand,
            std::vector<TString>& results) {
            if (currentLayer == chunks.size()) {
                results.emplace_back(JoinSeq("", currentCommand));
                return;
            }
            for (auto&& i : chunks[currentLayer]) {
                currentCommand.emplace_back(i);
                BuildVariantsImpl(chunks, currentLayer + 1, currentCommand, results);
                currentCommand.pop_back();
            }
        }
        std::vector<TString> BuildVariants(const TString& command) {
            auto chunks = StringSplitter(command).SplitByString("$$").ToList<TString>();
            std::vector<std::vector<TString>> chunksVariants;
            for (ui32 i = 0; i < chunks.size(); ++i) {
                if (i % 2 == 0) {
                    chunksVariants.emplace_back(std::vector<TString>({ chunks[i] }));
                } else {
                    chunksVariants.emplace_back(StringSplitter(chunks[i]).SplitBySet("|").ToList<TString>());
                }
            }
            std::vector<TString> result;
            std::vector<TString> currentCommand;
            BuildVariantsImpl(chunksVariants, 0, currentCommand, result);
            return result;
        }

    public:
        TScriptVariator(const TString& script) {
            auto commands = StringSplitter(script).SplitByString("------").ToList<TString>();
            std::vector<std::vector<std::shared_ptr<ICommand>>> commandsDescription;
            for (auto&& i : commands) {
                auto& cVariants = commandsDescription.emplace_back();
                i = Strip(i);
                std::vector<TString> variants = BuildVariants(i);
                for (auto&& v : variants) {
                    cVariants.emplace_back(BuildCommand(v));
                }
            }
            std::vector<TScriptExecutor> scripts;
            std::vector<std::shared_ptr<ICommand>> scriptCommands;
            BuildScripts(commandsDescription, 0, scriptCommands, Scripts);
        }

        void Execute() {
            for (auto&& i : Scripts) {
                i.Execute();
            }

        }
    };

    Y_UNIT_TEST(EmptyVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u), (2u), (3u), (4u)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (11u), (12u), (13u), (14u)
            ------
            ONE_COMPACTION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;#];[2u;#];[3u;#];[4u;#];[11u;#];[12u;#];[13u;#];[14u;#]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(EmptyStringVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5|1$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "", "b" : "", "c" : ""}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"\",\"b\":\"\",\"c\":\"\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(FilterVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2" ORDER BY Col1;
            EXPECTED: [[2u;["{\"a\":\"a2\"}"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(SimpleVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(CompactionVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a" : "1a1"}')), (12u, JsonDocument('{"a" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b" : "1b3"}')), (14u, JsonDocument('{"b" : "1b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
            ------
            ONE_COMPACTION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]];[10u;#];
                                   [11u;["{\"a\":\"1a1\"}"]];[12u;["{\"a\":\"1a2\"}"]];[13u;["{\"b\":\"1b3\"}"]];[14u;["{\"a\":\"a4\",\"b\":\"1b4\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(SwitchAccessorCompactionVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a" : "1a1"}')), (12u, JsonDocument('{"a" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b" : "1b3"}')), (14u, JsonDocument('{"b" : "1b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
            ------
            ONE_COMPACTION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]];[10u;#];
                                   [11u;["{\"a\":\"1a1\"}"]];[12u;["{\"a\":\"1a2\"}"]];[13u;["{\"b\":\"1b3\"}"]];[14u;["{\"a\":\"a4\",\"b\":\"1b4\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(DuplicationCompactionVariants) {
        TString script = R"(
            STOP_COMPACTION
            ------
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "1a1"}')), (2u, JsonDocument('{"a" : "1a2"}')), 
                                                                    (3u, JsonDocument('{"b" : "1b3"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(2u)
            ------
            ONE_COMPACTION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"1a1\"}"]];[2u;#];[3u;["{\"b\":\"1b3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

}

}   // namespace NKikimr::NKqp
