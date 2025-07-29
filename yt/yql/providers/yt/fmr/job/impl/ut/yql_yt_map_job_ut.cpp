#include "yql_yt_job_ut.h"

#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/split.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_printer.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/process/yql_yt_job_fmr.h>
#include <yt/yql/providers/yt/fmr/table_data_service/helpers/yql_yt_table_data_service_helpers.h>

using namespace NKikimr::NMiniKQL;

namespace NYql::NFmr {

TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                            "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";

Y_UNIT_TEST_SUITE(MapTests) {
    Y_UNIT_TEST(RunMapJob) {
        TFmrUserJob mapJob;

        TTempFileHandle tableDataServiceHostsFile;
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        std::vector<TTableDataServiceServerConnection> connections{{.Host = "localhost", .Port = port}};
        WriteHostsToFile(tableDataServiceHostsFile, 1, connections);

        TTempFileHandle inputYsonContentFile{};
        TFileOutput fileWriter(inputYsonContentFile.Name());
        fileWriter.Write(inputYsonContent.data(), inputYsonContent.size());
        fileWriter.Flush();

        TYtTableTaskRef fileTask{
            .RichPaths = {NYT::TRichYPath().Path("test_path").Cluster("test_cluster")},
            .FilePaths = {inputYsonContentFile.Name()}
        };
        TFmrTableOutputRef fmrOutputRef{.TableId = "table_id", .PartId = "part_id"};
        TTaskTableRef taskTableRef(fileTask);
        TMapTaskParams mapTaskParams{
            .Input = TTaskTableInputRef{.Inputs ={taskTableRef}},
            .Output = {fmrOutputRef}
        };
        FillMapFmrJob(mapJob, mapTaskParams, {}, tableDataServiceHostsFile.Name(), MakeFileYtJobSerivce());

        {
            auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),functionRegistry->SupportsSizedAllocators());
            alloc.Ref().ForcefullySetMemoryYellowZone(false);

            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, *functionRegistry);

            const auto structType = pgmBuilder.NewStructType({
                {"key", pgmBuilder.NewDataType(NUdf::EDataSlot::String)},
                {"subkey", pgmBuilder.NewDataType(NUdf::EDataSlot::String)},
                {"value", pgmBuilder.NewDataType(NUdf::EDataSlot::String)}
            });

            auto dataType = pgmBuilder.NewFlowType(structType);
            TCallableBuilder inputCallableBuilder(env, "FmrInputJob", dataType);
            auto inputNode = TRuntimeNode(inputCallableBuilder.Build(), false);

            const auto prefix = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("prefix_");
            const auto map = pgmBuilder.Map(inputNode,
                [&](TRuntimeNode item) {
                TRuntimeNode prefixKey = pgmBuilder.Concat(prefix, pgmBuilder.Member(item, "key"));
                TRuntimeNode subkey = pgmBuilder.Member(item, "subkey");
                TRuntimeNode value = pgmBuilder.Member(item, "value");
                return pgmBuilder.NewStruct(structType, {{"key", prefixKey}, {"subkey", subkey}, {"value", value}});
            });

            TCallableBuilder outputCallableBuilder(env, "FmrOutputJob", dataType);
            outputCallableBuilder.Add(map);
            auto outputNode = TRuntimeNode(outputCallableBuilder.Build(), false);
            auto pgmReturn = pgmBuilder.Discard(outputNode);

            TString serializedMapLambda = SerializeRuntimeNode(pgmReturn, env);
            mapJob.SetLambdaCode(serializedMapLambda);

            const TString spec = R"({
                tables = [{
                    "_yql_row_spec" = {
                        "Type" = [
                            "StructType"; [
                                ["key"; ["DataType"; "String"]];
                                ["subkey"; ["DataType"; "String"]];
                                ["value"; ["DataType"; "String"]];
                            ]
                        ]
                    }
                }]
            })";
            const TString type = R"(
            [
                "StructType";
                    [
                        ["key"; ["DataType"; "String"]];
                        ["subkey"; ["DataType"; "String"]];
                        ["value"; ["DataType"; "String"]];
                    ]
            ])";
            mapJob.SetInputSpec(spec);
            mapJob.SetOutSpec(spec);
            mapJob.SetInputType(type);
        }

        mapJob.DoFmrJob();

        // Checking correctness
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        TString key = "table_id_part_id:0";
        auto gottenBinaryTableContent = tableDataServiceClient->Get(key).GetValueSync();
        UNIT_ASSERT(gottenBinaryTableContent);

        // Reformating data
        auto textTableContent = GetTextYson(*gottenBinaryTableContent);
        THashMap<TString, std::vector<TString>> expectedFormattedContent{
            {"key", {"prefix_075", "prefix_800"}},
            {"subkey", {"1", "2"}},
            {"value", {"abc", "ddd"}}
        };
        THashMap<TString, std::vector<TString>> gottenFormattedContent;

        std::vector<TString> splittedTableContent;
        StringSplitter(textTableContent).SplitBySet("\n\";{}=").SkipEmpty().Collect(&splittedTableContent);

        for (ui64 i = 0; i < splittedTableContent.size(); i += 2) {
            TString columnKey = splittedTableContent[i], columnValue = splittedTableContent[i + 1];
            gottenFormattedContent[columnKey].emplace_back(columnValue);
        }
        UNIT_ASSERT_VALUES_EQUAL(expectedFormattedContent, gottenFormattedContent);
    }
}

} // namespace NYql::NFmr
