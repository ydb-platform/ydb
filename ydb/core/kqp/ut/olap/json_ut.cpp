#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/string_utils/base64/base64.h>
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
        TString Command;
        TString Compare;
        std::optional<ui64> ExpectIndexSkip;
        std::optional<ui64> ExpectIndexNoData;
        std::optional<ui64> ExpectIndexApprove;

        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
            AFL_VERIFY(controller);
            const i64 indexSkipStart = controller->GetIndexesSkippingOnSelect().Val();
            const i64 indexApproveStart = controller->GetIndexesApprovedOnSelect().Val();
            const i64 indexNoDataStart = controller->GetIndexesSkippedNoData().Val();

            const i64 headerSkipStart = controller->GetHeadersSkippingOnSelect().Val();
            const i64 headerApproveStart = controller->GetHeadersApprovedOnSelect().Val();
            const i64 headerNoDataStart = controller->GetHeadersSkippedNoData().Val();

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
            const ui32 iSkip = controller->GetIndexesSkippingOnSelect().Val() - indexSkipStart;
            const ui32 iNoData = controller->GetIndexesSkippedNoData().Val() - indexNoDataStart;
            const ui32 iApproves = controller->GetIndexesApprovedOnSelect().Val() - indexApproveStart;
            Cerr << "INDEX:" << iNoData << "/" << iSkip << "/" << iApproves << Endl;

            const ui32 hSkip = controller->GetHeadersSkippingOnSelect().Val() - headerSkipStart;
            const ui32 hNoData = controller->GetHeadersSkippedNoData().Val() - headerNoDataStart;
            const ui32 hApproves = controller->GetHeadersApprovedOnSelect().Val() - headerApproveStart;
            Cerr << "HEADER:" << hNoData << "/" << hSkip << "/" << hApproves << Endl;
            if (ExpectIndexSkip) {
                AFL_VERIFY(iSkip + hSkip == *ExpectIndexSkip)("expect", ExpectIndexSkip)("ireal", iSkip)("hreal", hSkip)(
                                            "current", controller->GetIndexesSkippingOnSelect().Val())("pred", indexSkipStart);
            }
            if (ExpectIndexNoData) {
                AFL_VERIFY(iNoData == *ExpectIndexNoData)("expect", ExpectIndexNoData)("real", iNoData)(
                                        "current", controller->GetIndexesSkippedNoData().Val())("pred", indexNoDataStart);
            }
            if (ExpectIndexApprove) {
                AFL_VERIFY(iApproves == *ExpectIndexApprove)("expect", ExpectIndexApprove)("real", iApproves)(
                                          "current", controller->GetIndexesApprovedOnSelect().Val())("pred", indexApproveStart);
            }
            return TConclusionStatus::Success();
        }

    public:
        bool DeserializeFromString(const TString& info) {
            auto lines = StringSplitter(info).SplitBySet("\n").ToList<TString>();
            std::optional<ui32> state;
            for (auto&& l : lines) {
                l = Strip(l);
                if (l.StartsWith("READ:")) {
                    l = l.substr(5);
                    state = 0;
                } else if (l.StartsWith("EXPECTED:")) {
                    l = l.substr(9);
                    state = 1;
                } else if (l.StartsWith("IDX_ND_SKIP_APPROVE:")) {
                    state = 2;
                    l = l.substr(20);
                } else {
                    AFL_VERIFY(state)("line", l);
                }

                if (*state == 0) {
                    Command += l;
                } else if (*state == 1) {
                    Compare += l;
                } else if (*state == 2) {
                    auto idxExpectations = StringSplitter(l).SplitBySet(" ,.;").SkipEmpty().ToList<TString>();
                    AFL_VERIFY(idxExpectations.size() == 3)("size", idxExpectations.size())("string", l);
                    if (idxExpectations[0] != "{}") {
                        ui32 res;
                        AFL_VERIFY(TryFromString<ui32>(idxExpectations[0], res))("string", l);
                        ExpectIndexNoData = res;
                    }
                    if (idxExpectations[1] != "{}") {
                        ui32 res;
                        AFL_VERIFY(TryFromString<ui32>(idxExpectations[1], res))("string", l);
                        ExpectIndexSkip = res;
                    }
                    if (idxExpectations[2] != "{}") {
                        ui32 res;
                        AFL_VERIFY(TryFromString<ui32>(idxExpectations[2], res))("string", l);
                        ExpectIndexApprove = res;
                    }
                } else {
                    AFL_VERIFY(false)("line", l);
                }
            }
            return true;
        }

        TSelectCommand() = default;
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

    class TOneActualizationCommand: public ICommand {
    private:
        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            {
                auto alterQuery =
                    TStringBuilder()
                    << "ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                AFL_VERIFY(alterResult.GetStatus() == NYdb::EStatus::SUCCESS)("error", alterResult.GetIssues().ToString());
            }
            auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
            AFL_VERIFY(controller);
            controller->WaitActualization(TDuration::Seconds(10));
            return TConclusionStatus::Success();
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

    class TBulkUpsertCommand: public ICommand {
    private:
        TString TableName;
        TString ArrowBatch;
        Ydb::StatusIds_StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS;

    public:
        TBulkUpsertCommand() = default;

        virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
            TLocalHelper lHelper(kikimr);
            lHelper.SendDataViaActorSystem(TableName,
                NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TNativeSerializer().Deserialize(ArrowBatch)), ExpectedCode);
            return TConclusionStatus::Success();
        }

        bool DeserializeFromString(const TString& info) {
            auto lines = StringSplitter(info).SplitBySet("\n").SkipEmpty().ToList<TString>();
            if (lines.size() < 2 || lines.size() > 3) {
                return false;
            }
            TableName = Strip(lines[0]);
            ArrowBatch = Base64Decode(Strip(lines[1]));
            AFL_VERIFY(!!ArrowBatch);
            if (lines.size() == 3) {
                if (!Ydb::StatusIds_StatusCode_Parse(Strip(lines[2]), &ExpectedCode)) {
                    return false;
                }
//                if (lines[2] == "SUCCESS") {
//                } else if (lines[2] = "INTERNAL_ERROR") {
//                    ExpectedCode = Ydb::StatusIds::INTERNAL_ERROR;
//                } else if (lines[2] == "BAD_REQUEST") {
//                    ExpectedCode = Ydb::StatusIds::BAD_REQUEST;
//                } else {
//                    return false;
//                }
            }
            return true;
        }
    };

    class TScriptExecutor {
    private:
        std::vector<std::shared_ptr<ICommand>> Commands;

    public:
        TScriptExecutor(const std::vector<std::shared_ptr<ICommand>>& commands)
            : Commands(commands) {
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
            AFL_VERIFY(NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val() == 0)("count",
                           NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val());
            AFL_VERIFY(NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val() == 0)(
                           "count", NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val());
        }
    };

    class TScriptVariator {
    private:
        std::vector<TScriptExecutor> Scripts;
        std::shared_ptr<ICommand> BuildCommand(TString command) {
            if (command.StartsWith("BULK_UPSERT:")) {
                command = command.substr(12);
                auto result = std::make_shared<TBulkUpsertCommand>();
                AFL_VERIFY(result->DeserializeFromString(command));
                return result;
            } else if (command.StartsWith("SCHEMA:")) {
                command = command.substr(7);
                return std::make_shared<TSchemaCommand>(command);
            } else if (command.StartsWith("DATA:")) {
                command = command.substr(5);
                return std::make_shared<TDataCommand>(command);
            } else if (command.StartsWith("READ:")) {
                auto result = std::make_shared<TSelectCommand>();
                AFL_VERIFY(result->DeserializeFromString(command));
                return result;
            } else if (command.StartsWith("WAIT_COMPACTION")) {
                return std::make_shared<TWaitCompactionCommand>();
            } else if (command.StartsWith("STOP_COMPACTION")) {
                return std::make_shared<TStopCompactionCommand>();
            } else if (command.StartsWith("ONE_COMPACTION")) {
                return std::make_shared<TOneCompactionCommand>();
            } else if (command.StartsWith("ONE_ACTUALIZATION")) {
                return std::make_shared<TOneActualizationCommand>();
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
            auto lines = StringSplitter(script).SplitByString("\n").ToList<TString>();
            lines.erase(std::remove_if(lines.begin(), lines.end(),
                            [](const TString& l) {
                                return Strip(l).StartsWith("#");
                            }),
                lines.end());
            auto commands = StringSplitter(JoinSeq("\n", lines)).SplitByString("------").ToList<TString>();
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `FORCE_SIMD_PARSING`=`$$true|false$$`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5|1$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (1u, JsonDocument('{"a" : "", "b" : "", "c" : ""}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"\",\"b\":\"\",\"c\":\"\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(QuotedFilterVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1", "b.c.d" : "b1", "c.d.e" : "c1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')),
                                                                    (3u, JsonDocument('{"b.c.d" : "b3", "d.e.f" : "d3"}')), (4u, JsonDocument('{"b.c.d" : "b4asdsasdaa", "a.b.c" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a2" ORDER BY Col1;
            EXPECTED: [[2u;["{\"a.b.c\":\"a2\"}"]]]
            
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, 
                      `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                      `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2" ORDER BY Col1;
            EXPECTED: [[2u;["{\"a\":\"a2\"}"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".c") = "1" ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"e.v\".e.\"c.a\"") = "2" ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(RestoreFirstLevelVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`true`, 
                      `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                      `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3", "e" : ["a", {"v" : ["c", 5]}]}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":\"{\\\"c\\\":1,\\\"e\\\":{\\\"c.a\\\":2}}\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\",\"e\":\"[\\\"a\\\",{\\\"v\\\":[\\\"c\\\",5]}]\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(RestoreFullJsonVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, 
                      `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                      `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1", "d" : null, "e.v" : {"c" : 1, "e" : {"c.a" : 2}}}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3", "e" : ["a", {"v" : ["c", 5]}]}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\",\"d\":\"NULL\",\"e.v\":{\"c\":\"1\",\"e\":{\"c.a\":\"2\"}}}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\",\"e\":[\"a\",{\"v\":[\"c\",\"5\"]}]}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(BrokenJsonWriting) {
        NColumnShard::TTableUpdatesBuilder updates(NArrow::MakeArrowSchema(
            { { "Col1", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64) }, { "Col2", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } }));
        updates.AddRow().Add<int64_t>(1).Add("{\"a\" : \"c}");
        auto arrowString = Base64Encode(NArrow::NSerialization::TNativeSerializer().SerializeFull(updates.BuildArrow()));

        TString script = Sprintf(R"(
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, 
                      `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                      `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            BULK_UPSERT:
                /Root/ColumnTable
                %s
                BAD_REQUEST
        )",
            arrowString.data());
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(RestoreJsonArrayVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY`=`false`, 
                      `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`,
                      `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('["a", {"v" : 4}, 1,2,3,4,5,6,7,8,9,10,11,12]'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["[\"a\",{\"v\":\"4\"},\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\",\"11\",\"12\"]"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(DoubleFilterVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.b") = "b3" AND JSON_VALUE(Col2, "$.d") = "d3" ORDER BY Col1;
            EXPECTED: [[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]]]
            ------
            READ: SELECT * FROM `/Root/ColumnTable` ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}"]];[2u;["{\"a\":\"a2\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(OrFilterVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE (JSON_VALUE(Col2, "$.b") = "b3" AND JSON_VALUE(Col2, "$.d") = "d3") OR (JSON_VALUE(Col2, "$.b") = "b1" AND JSON_VALUE(Col2, "$.c") = "c1") ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\",\"b\":\"b1\",\"c\":\"c1\"}"]];[3u;["{\"b\":\"b3\",\"d\":\"d3\"}"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(DoubleFilterReduceScopeVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                Col3 UTF8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                    (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4"}'), "value4")
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND Col3 = "value2" ORDER BY Col1;
            EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(DoubleFilterReduceScopeWithPredicateVariants) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                Col3 UTF8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                    (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4"}'), "value4")
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND Col3 = "value2" AND Col1 > 1 ORDER BY Col1;
            EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(DoubleFilterReduceScopeWithPredicateVariantsWithSeparatedColumnAtFirst) {
        TString script = R"(
            SCHEMA:            
            CREATE TABLE `/Root/ColumnTable` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                Col3 UTF8,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1|2|10$$);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES(1u, JsonDocument('{"a" : "value_a", "b" : "b1", "c" : "c1"}'), "value1"), (2u, JsonDocument('{"a" : "value_a"}'), "value1"),
                                                                    (3u, JsonDocument('{"a" : "value_a", "b" : "value_b"}'), "value2"), (4u, JsonDocument('{"b" : "value_b", "a" : "a4dsadasdasdasdsdasdasdas"}'), "value4")
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "value_a" AND JSON_VALUE(Col2, "$.b") = "value_b" AND Col1 > 1 ORDER BY Col1;
            EXPECTED: [[3u;["{\"a\":\"value_a\",\"b\":\"value_b\"}"];["value2"]]]
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(FilterVariantsCount) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1", "b" : "b1", "c" : "c1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3", "d" : "d3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT COUNT(*) FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a2";
            EXPECTED: [[1u]]
            
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
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

    Y_UNIT_TEST(SimpleExistsVariants) {
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$1024|0|1$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a" : "a1"}')), (2u, JsonDocument('{"a" : "a2"}')),
                                                                    (3u, JsonDocument('{"b" : "b3"}')), (4u, JsonDocument('{"b" : "b4asdsasdaa", "a" : "a4"}'))
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_EXISTS(Col2, "$.a") ORDER BY Col1;
            EXPECTED: [[1u;["{\"a\":\"a1\"}"]];[2u;["{\"a\":\"a2\"}"]];[4u;["{\"a\":\"a4\",\"b\":\"b4asdsasdaa\"}"]]]
            
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
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

    Y_UNIT_TEST(BloomMixIndexesVariants) {
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
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                      `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=a_index, TYPE=$$CATEGORY_BLOOM_FILTER|BLOOM_FILTER$$,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_b, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096, 
                           "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_a, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096, 
                           "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
            ------
            ONE_ACTUALIZATION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
            EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=DROP_INDEX, NAME=a_index)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "1a1" ORDER BY Col1;
            EXPECTED: [[11u;["{\"a.b.c\":\"1a1\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=b_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "%1b4%" ORDER BY Col1;
            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1b4%" ORDER BY Col1;
#            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
#            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1B4" ORDER BY Col1;
#            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
#            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "1b5" ORDER BY Col1;
#            EXPECTED: []
#            IDX_ND_SKIP_APPROVE: 0, 5, 0
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
#            EXPECTED: []
#            IDX_ND_SKIP_APPROVE: 0, 5, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
            EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 3, 2
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 5, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b3" ORDER BY Col1;
            EXPECTED: [[13u;["{\"b.c.d\":\"1b3\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 5, 0
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(BloomCategoryIndexesVariants) {
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
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                      `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=a_index, TYPE=$$CATEGORY_BLOOM_FILTER|BLOOM_FILTER$$,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
            ------
            ONE_ACTUALIZATION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
            EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=DROP_INDEX, NAME=a_index)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "1a1" ORDER BY Col1;
            EXPECTED: [[11u;["{\"a.b.c\":\"1a1\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=b_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`)
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 5, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
            EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 3, 2
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 5, 0
            
        )";
        TScriptVariator(script).Execute();
    }

    Y_UNIT_TEST(BloomNGrammIndexesVariants) {
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
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`, `FORCE_SIMD_PARSING`=`$$true|false$$`, `SCAN_FIRST_LEVEL_ONLY`=`$$true|false$$`,
                      `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10$$`,
                      `MEM_LIMIT_CHUNK`=`$$0|1000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(1u, JsonDocument('{"a.b.c" : "a1"}')), (2u, JsonDocument('{"a.b.c" : "a2"}')), 
                                                                    (3u, JsonDocument('{"b.c.d" : "b3"}')), (4u, JsonDocument('{"b.c.d" : "b4", "a" : "a4"}'))
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES(11u, JsonDocument('{"a.b.c" : "1a1"}')), (12u, JsonDocument('{"a.b.c" : "1a2"}')), 
                                                                    (13u, JsonDocument('{"b.c.d" : "1b3"}')), (14u, JsonDocument('{"b.c.d" : "1b4", "a" : "a4"}'))
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_b, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096, 
                           "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_a, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096, 
                           "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
            ------
            DATA:
            REPLACE INTO `/Root/ColumnTable` (Col1) VALUES(10u)
            ------
            ONE_ACTUALIZATION
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1;
            EXPECTED: [[1u;["{\"a.b.c\":\"a1\"}"]]]
            IDX_ND_SKIP_APPROVE: 5, 0, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1;
            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "%1b4%" ORDER BY Col1;
            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1b4%" ORDER BY Col1;
#            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
#            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1B4" ORDER BY Col1;
#            EXPECTED: [[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
#            IDX_ND_SKIP_APPROVE: 0, 4, 1
#            ------
#            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "1b5" ORDER BY Col1;
#            EXPECTED: []
#            IDX_ND_SKIP_APPROVE: 0, 5, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 5, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1;
            EXPECTED: [[4u;["{\"a\":\"a4\",\"b.c.d\":\"b4\"}"]];[14u;["{\"a\":\"a4\",\"b.c.d\":\"1b4\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 3, 2
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 5, 0, 0
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b3" ORDER BY Col1;
            EXPECTED: [[13u;["{\"b.c.d\":\"1b3\"}"]]]
            IDX_ND_SKIP_APPROVE: 0, 4, 1
            ------
            READ: SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1B3" ORDER BY Col1;
            EXPECTED: []
            IDX_ND_SKIP_APPROVE: 0, 4, 1
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
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
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
            ------
            SCHEMA:
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, 
                      `FORCE_SIMD_PARSING`=`$$true|false$$`, `COLUMNS_LIMIT`=`$$0|1|1024$$`, `SPARSED_DETECTOR_KFF`=`$$0|10|1000$$`, `MEM_LIMIT_CHUNK`=`$$0|100|1000000$$`, `OTHERS_ALLOWED_FRACTION`=`$$0|0.5$$`)
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
