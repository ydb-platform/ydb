#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_mock.h>

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
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        std::unordered_map<TYtTableRef, TString> inputTables{{input, TableContent_1}};
        std::unordered_map<TYtTableRef, TString> outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        auto res = job->Download(params);

        auto err = std::get_if<TError>(&res);
        auto statistics = std::get_if<TStatistics>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        UNIT_ASSERT_EQUAL(statistics->OutputTables.at(output).Rows, 4);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, GetBinaryYson(TableContent_1));
    }

    Y_UNIT_TEST(UploadTable) {
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        std::unordered_map<TYtTableRef, TString> inputTables, outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);

        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TYtTableRef output = TYtTableRef("test_cluster", "test_path");
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        auto params = TUploadTaskParams(input, output);

        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key, GetBinaryYson(TableContent_1));

        auto res = job->Upload(params);

        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        UNIT_ASSERT(outputTables.contains(output));
        UNIT_ASSERT_NO_DIFF(GetTextYson(outputTables[output]), TableContent_1);
    }

    Y_UNIT_TEST(MergeMixedTables) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        std::unordered_map<TYtTableRef, TString> inputTables{{input_2, TableContent_2}};
        std::unordered_map<TYtTableRef, TString> outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key_1, GetBinaryYson(TableContent_1));
        tableDataServicePtr->Put(key_3, GetBinaryYson(TableContent_3));

        auto res = job->Merge(params);
        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err, err->ErrorMessage);
        auto resultTableContentMaybe = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContentMaybe, "Result table content is empty");
        TString resultTableContent = GetTextYson(*resultTableContentMaybe);
        UNIT_ASSERT_NO_DIFF(resultTableContent, TableContent_1 + TableContent_2 + TableContent_3);
    }
}

Y_UNIT_TEST_SUITE(TaskRunTests) {
    Y_UNIT_TEST(RunDownloadTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        std::unordered_map<TYtTableRef, TString> inputTables{{input, TableContent_1}};
        std::unordered_map<TYtTableRef, TString> outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Download, "test_task_id", params, "test_session_id");
        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(GetTextYson(*resultTableContent), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        std::unordered_map<TYtTableRef, TString> inputTables, outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");
        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key, GetBinaryYson(TableContent_1));
        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT(outputTables.contains(output));
        UNIT_ASSERT_NO_DIFF(GetTextYson(outputTables[output]), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        std::unordered_map<TYtTableRef, TString> inputTables, outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        // No tables in tableDataService
        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;
        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
    }

    Y_UNIT_TEST(RunMergeTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        std::unordered_map<TYtTableRef, TString> inputTables{{input_2, TableContent_2}};
        std::unordered_map<TYtTableRef, TString> outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key_1, GetBinaryYson(TableContent_1));
        tableDataServicePtr->Put(key_3, GetBinaryYson(TableContent_3));

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(GetTextYson(*resultTableContent), TableContent_1 + TableContent_2 + TableContent_3);
    }

    Y_UNIT_TEST(RunMergeTaskWithNoTable) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        std::unordered_map<TYtTableRef, TString> inputTables, outputTables;
        NYql::NFmr::IYtService::TPtr ytService = MakeMockYtService(inputTables, outputTables);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key_1, GetBinaryYson(TableContent_1));
        // No table (input_2) in Yt
        tableDataServicePtr->Put(key_3, GetBinaryYson(TableContent_3));

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT(!resultTableContent);
    }
}

} // namespace NYql
