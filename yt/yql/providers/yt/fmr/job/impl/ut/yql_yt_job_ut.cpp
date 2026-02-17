#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>

#include <util/stream/file.h>

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

Y_UNIT_TEST_SUITE(FmrJobTests) {
    Y_UNIT_TEST(DownloadTable) {
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TTempFileHandle ytInputFile;
        {
            TFileOutput out(ytInputFile.Name());
            out.Write(TableContent_1.data(), TableContent_1.size());
        }

        TYtTableTaskRef input = TYtTableTaskRef{.RichPaths = {richPath}, .FilePaths = {ytInputFile.Name()}};
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TString tableDataServiceExpectedOutputGroup = GetTableDataServiceGroup(output.TableId, output.PartId);
        TString talblDataServiceExpectedOutputChunkId = "0";

        auto res = job->Download(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);

        auto err = std::get_if<TFmrError>(&res);
        auto statistics = std::get_if<TStatistics>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        auto detailedChunkStats = statistics->OutputTables.at(output).PartIdChunkStats;
        UNIT_ASSERT_VALUES_EQUAL(detailedChunkStats.size(), 1); // coordinator settings taken from file with default values, so large chunk size
        UNIT_ASSERT_VALUES_EQUAL(detailedChunkStats[0].Rows, 4);

        auto resultTableContent = tableDataServiceClient->Get(tableDataServiceExpectedOutputGroup, talblDataServiceExpectedOutputChunkId).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(GetTextYson(*resultTableContent), TableContent_1);
    }

    Y_UNIT_TEST(UploadTable) {
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();

        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TTempFileHandle ytOutputFile;
        TYtTableRef output = TYtTableRef("test_cluster", "test_path", ytOutputFile.Name());
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        auto params = TUploadTaskParams(input, output);

        TString group = GetTableDataServiceGroup(input.TableId, "test_part_id");
        TString chunkId = "0";
        tableDataServiceClient->Put(group, chunkId, GetBinaryYson(TableContent_1));

        auto res = job->Upload(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);

        auto err = std::get_if<TFmrError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        const TString resultFileContent = TFileInput(ytOutputFile.Name()).ReadAll();
        TString expectedLine;
        TStringStream expectedStream(TableContent_1);
        while (expectedStream.ReadLine(expectedLine)) {
            UNIT_ASSERT_C(resultFileContent.Contains(expectedLine), TStringBuilder() << "Missing line in output: " << expectedLine);
        }
    }

    Y_UNIT_TEST(MergeMixedTables) {
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TTempFileHandle ytInputFile;
        {
            TFileOutput out(ytInputFile.Name());
            out.Write(TableContent_2.data(), TableContent_2.size());
        }
        TYtTableTaskRef input_2 = TYtTableTaskRef{.RichPaths = {richPath}, .FilePaths = {ytInputFile.Name()}};
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        IFmrJob::TPtr job = MakeFmrJob(file.Name(), ytJobService, jobLauncher);

        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams{.Input = TTaskTableInputRef{.Inputs = inputs}, .Output = output};
        auto tableDataServiceExpectedOutputGroup = GetTableDataServiceGroup(output.TableId, output.PartId);

        auto group_1 = GetTableDataServiceGroup(input_1.TableId, "test_part_id");
        auto group_3 = GetTableDataServiceGroup(input_3.TableId, "test_part_id");
        auto chunkId = "0";
        tableDataServiceClient->Put(group_1, chunkId, GetBinaryYson(TableContent_1));
        tableDataServiceClient->Put(group_3, chunkId, GetBinaryYson(TableContent_3));

        auto res = job->Merge(params, {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}}, cancelFlag);
        auto err = std::get_if<TFmrError>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        auto resultTableContentMaybe = tableDataServiceClient->Get(tableDataServiceExpectedOutputGroup, chunkId).GetValueSync();
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
        TTempFileHandle ytInputFile;
        {
            TFileOutput out(ytInputFile.Name());
            out.Write(TableContent_1.data(), TableContent_1.size());
        }
        TYtTableTaskRef input = TYtTableTaskRef{.RichPaths = {richPath}, .FilePaths = {ytInputFile.Name()}};
        TFmrTableId inputFmrId("test_cluster", "test_path");
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        TString tableDataServiceExpectedOutputGroup = GetTableDataServiceGroup(output.TableId, output.PartId);
        TString talblDataServiceExpectedOutputChunkId = "0";
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Download, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServiceClient->Get(tableDataServiceExpectedOutputGroup, talblDataServiceExpectedOutputChunkId).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(GetTextYson(*resultTableContent), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTask) {
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TTempFileHandle ytOutputFile;
        TYtTableRef output = TYtTableRef("test_cluster", "test_path", ytOutputFile.Name());

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto group = GetTableDataServiceGroup(input.TableId, "test_part_id");
        TString chunk = "0";
        tableDataServiceClient->Put(group, chunk, GetBinaryYson(TableContent_1));
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        const TString resultFileContent = TFileInput(ytOutputFile.Name()).ReadAll();
        TString expectedLine;
        TStringStream expectedStream(TableContent_1);
        while (expectedStream.ReadLine(expectedLine)) {
            UNIT_ASSERT_C(resultFileContent.Contains(expectedLine), TStringBuilder() << "Missing line in output: " << expectedLine);
        }
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TTempFileHandle ytOutputFile;
        TYtTableRef output = TYtTableRef("test_cluster", "test_path", ytOutputFile.Name());

        TPortManager pm;
        const ui16 port = pm.GetPort();
        TTempFileHandle file;
        SetupTableDataServiceDiscovery(file, port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);
        auto tableDataServiceServer = MakeTableDataServiceServer(port);

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});

        TJobResult jobResult = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag);
        UNIT_ASSERT(jobResult.Error.Defined());
        auto error = *jobResult.Error;
        UNIT_ASSERT_EQUAL(error.Reason, EFmrErrorReason::RestartQuery);
        UNIT_ASSERT(error.ErrorMessage.Contains("Error reading chunk: test_table_id_test_part_id:0"));
    }

    Y_UNIT_TEST(RunMergeTask) {
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TTempFileHandle ytInputFile;
        {
            TFileOutput out(ytInputFile.Name());
            out.Write(TableContent_2.data(), TableContent_2.size());
        }
        TYtTableTaskRef input_2 = TYtTableTaskRef{.RichPaths = {richPath}, .FilePaths = {ytInputFile.Name()}};
        TFmrTableId inputFmrId_2("test_cluster", "test_path");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        NYql::NFmr::IYtJobService::TPtr ytJobService = MakeFileYtJobService();
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
        TString tableDataServiceExpectedOutputGroup = GetTableDataServiceGroup(output.TableId, output.PartId);
        TString chunk = "0";

        TTask::TPtr task = MakeTask(ETaskType::Merge, "test_task_id", params, "test_session_id", {{TFmrTableId("test_cluster", "test_path"), TClusterConnection()}});

        auto group_1 = GetTableDataServiceGroup(input_1.TableId, "test_part_id");
        auto group_3 = GetTableDataServiceGroup(input_3.TableId, "test_part_id");
        tableDataServiceClient->Put(group_1, chunk, GetBinaryYson(TableContent_1));
        tableDataServiceClient->Put(group_3, chunk, GetBinaryYson(TableContent_3));
        auto jobLauncher = MakeIntrusive<TFmrUserJobLauncher>(TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
        ETaskStatus status = RunJob(task, file.Name(), ytJobService, jobLauncher, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContentMaybe = tableDataServiceClient->Get(tableDataServiceExpectedOutputGroup, chunk).GetValueSync();
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
