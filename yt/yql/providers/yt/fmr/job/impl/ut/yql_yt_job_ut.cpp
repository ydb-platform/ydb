#include "yql_yt_job_ut.h"

#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/helpers/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/mock/yql_yt_job_service_mock.h>

namespace NYql::NFmr {

TString TableContent_1 = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n"
                        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};\n"
                        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};\n";
TString TableContent_2 = "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n"
                        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};\n"
                        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};\n";
TString TableContent_3 = "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n"
                        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};\n"
                        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};\n";

// TODO - make better setup to avoid duplication

TString GetBinaryYson(const TString& textYsonContent) {
    TStringStream binaryYsonInputStream;
    TStringStream textYsonInputStream(textYsonContent);
    NYson::ReformatYsonStream(&textYsonInputStream, &binaryYsonInputStream, NYson::EYsonFormat::Binary, ::NYson::EYsonType::ListFragment);
    return binaryYsonInputStream.ReadAll();
}

TString GetTextYson(const TString& binaryYsonContent) {
    TStringStream binaryYsonInputStream(binaryYsonContent);
    TStringStream textYsonInputStream;
    NYson::ReformatYsonStream(&binaryYsonInputStream, &textYsonInputStream, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
    return textYsonInputStream.ReadAll();
}

Y_UNIT_TEST_SUITE(FmrJobTests) {
    Y_UNIT_TEST(DownloadTable) {
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TYtTableTaskRef input = TYtTableTaskRef{.RichPaths = {richPath}};
        std::unordered_map<TString, TString> inputTables{{NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath)), TableContent_1}};
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        auto res = job->Download(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);

        auto err = std::get_if<TError>(&res);
        auto statistics = std::get_if<TStatistics>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        auto detailedChunkStats = statistics->OutputTables.at(output).PartIdChunkStats;
        UNIT_ASSERT_VALUES_EQUAL(detailedChunkStats.size(), 1); // coordinator settings taken from file with default values, so large chunk size
        UNIT_ASSERT_VALUES_EQUAL(detailedChunkStats[0].Rows, 4);

        auto resultTableContent = tableDataServiceClient->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, GetBinaryYson(TableContent_1));
    }

    Y_UNIT_TEST(UploadTable) {
        std::unordered_map<TString, TString> inputTables;
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);

        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TYtTableRef output = TYtTableRef{.RichPath = NYT::TRichYPath().Path("test_path").Cluster("test_cluster")};
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        auto params = TUploadTaskParams(input, output);

        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        tableDataServiceClient->Put(key, GetBinaryYson(TableContent_1));

        auto res = job->Upload(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);

        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        TString serailizedRichPath = SerializeRichPath(output.RichPath);
        UNIT_ASSERT(outputTables.contains(serailizedRichPath));
        UNIT_ASSERT_NO_DIFF(GetTextYson(outputTables[serailizedRichPath]), TableContent_1);
    }

    Y_UNIT_TEST(MergeMixedTables) {
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TYtTableTaskRef input_2 = TYtTableTaskRef{.RichPaths = {richPath}};
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        std::unordered_map<TString, TString> inputTables{{NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath)), TableContent_2}};
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams{.Input = TTaskTableInputRef{.Inputs = inputs}, .Output = output};
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServiceClient->Put(key_1, GetBinaryYson(TableContent_1));
        tableDataServiceClient->Put(key_3, GetBinaryYson(TableContent_3));

        auto res = job->Merge(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);
        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        auto resultTableContentMaybe = tableDataServiceClient->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContentMaybe, "Result table content is empty");
        TString resultTableContent = GetTextYson(*resultTableContentMaybe);
        TString expected = TableContent_1 + TableContent_2 + TableContent_3;
        UNIT_ASSERT_VALUES_EQUAL(resultTableContent.size(), expected.size());
        TString line;
        TStringStream resultStream(resultTableContent);
        while (resultStream.ReadLine(line)) {
            UNIT_ASSERT(expected.Contains(line));
        }
    }
}

Y_UNIT_TEST_SUITE(TaskRunTests) {
    Y_UNIT_TEST(RunDownloadTask) {
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TYtTableTaskRef input = TYtTableTaskRef{.RichPaths = {richPath}};
        TFmrTableId inputFmrId("test_cluster", "test_path");
        std::unordered_map<TString, TString> inputTables{{NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath)), TableContent_1}};
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Download, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServiceClient->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(GetTextYson(*resultTableContent), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTask) {
        std::unordered_map<TString, TString> inputTables;
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef{.RichPath = NYT::TRichYPath().Path("test_path").Cluster("test_cluster")};

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        tableDataServiceClient->Put(key, GetBinaryYson(TableContent_1));
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        TString serailizedRichPath = SerializeRichPath(output.RichPath);
        UNIT_ASSERT(outputTables.contains(serailizedRichPath));
        UNIT_ASSERT_NO_DIFF(GetTextYson(outputTables[serailizedRichPath]), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {
        std::unordered_map<TString, TString> inputTables;
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef{.RichPath = NYT::TRichYPath().Path("test_path").Cluster("test_cluster")};

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);

        // No tables in tableDataService
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag),
            yexception,
            "No data for chunk:test_table_id_test_part_id"
        );
    }

    Y_UNIT_TEST(RunMergeTask) {
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TYtTableTaskRef input_2 = TYtTableTaskRef{.RichPaths = {richPath}};
        TFmrTableId inputFmrId_2("test_cluster", "test_path");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        std::unordered_map<TString, TString> inputTables{{NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath)), TableContent_2}};
        std::unordered_map<TString, TString> outputTables;
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeMockYtJobService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams{.Input = TTaskTableInputRef{.Inputs = inputs}, .Output = output};
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        TTask::TPtr task = MakeTask(ETaskType::Merge, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServiceClient->Put(key_1, GetBinaryYson(TableContent_1));
        tableDataServiceClient->Put(key_3, GetBinaryYson(TableContent_3));
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(false);
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContentMaybe = tableDataServiceClient->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContentMaybe, "Result table content is empty");

        TString resultTableContent = GetTextYson(*resultTableContentMaybe);
        TString expected = TableContent_1 + TableContent_2 + TableContent_3;
        UNIT_ASSERT_VALUES_EQUAL(resultTableContent.size(), expected.size());
        TString line;
        TStringStream resultStream(resultTableContent);
        while (resultStream.ReadLine(line)) {
            UNIT_ASSERT(expected.Contains(line));
        }
    }
}

} // namespace NYql
