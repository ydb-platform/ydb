#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_mock.h>

namespace NYql::NFmr {

TString TableContent_1 = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
                        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
                        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
                        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
TString TableContent_2 = "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
                        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
                        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};"
                        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
TString TableContent_3 = "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
                        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
                        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};"
                        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

Y_UNIT_TEST_SUITE(FmrJobTests) {
    Y_UNIT_TEST(DownloadTable) {
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TFmrJobSettings settings = {1};
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag, settings);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        ytUploadedTablesMock->AddTable(input, TableContent_1);

        auto res = job->Download(params);

        auto err = std::get_if<TError>(&res);
        auto statistics = std::get_if<TStatistics>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        UNIT_ASSERT_EQUAL(statistics->OutputTables.at(output).Rows, 4);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, TableContent_1);
    }

    Y_UNIT_TEST(UploadTable) {
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TFmrJobSettings settings = {1};
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag, settings);

        TYtTableRef output = TYtTableRef("test_cluster", "test_path");
        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        auto params = TUploadTaskParams(input, output);

        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key, TableContent_1);

        auto res = job->Upload(params);

        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), TableContent_1);
    }

    Y_UNIT_TEST(MergeFmrTables) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TFmrJobSettings settings = {1};
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag, settings);

        std::vector<TTableRange> ranges = {{"test_part_id"}};

        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1", .TableRanges = ranges};
        TFmrTableInputRef input_2 = TFmrTableInputRef{.TableId = "test_table_id_2", .TableRanges = ranges};
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3", .TableRanges = ranges};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id_output", "test_part_id");
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_2 = GetTableDataServiceKey(input_2.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key_1, TableContent_1);
        tableDataServicePtr->Put(key_2, TableContent_2);
        tableDataServicePtr->Put(key_3, TableContent_3);

        auto res = job->Merge(params);

        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, TableContent_1 + TableContent_2 + TableContent_3);
    }

    Y_UNIT_TEST(MergeMixedTables) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        TFmrJobSettings settings = {1};
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag, settings);

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

        auto key_1 = GetTableDataServiceKey(input_1.TableId, "test_part_id", 0);
        auto key_3 = GetTableDataServiceKey(input_3.TableId, "test_part_id", 0);
        tableDataServicePtr->Put(key_1, TableContent_1);
        ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(key_3, TableContent_3);

        auto res = job->Merge(params);

        auto err = std::get_if<TError>(&res);

        UNIT_ASSERT_C(!err,err->ErrorMessage);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, TableContent_1 + TableContent_2 + TableContent_3);
    }
}

Y_UNIT_TEST_SUITE(TaskRunTests) {
    Y_UNIT_TEST(RunDownloadTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableOutputRef output = TFmrTableOutputRef("test_table_id", "test_part_id");
        auto tableDataServiceExpectedOutputKey = GetTableDataServiceKey(output.TableId, output.PartId, 0);

        ytUploadedTablesMock->AddTable(input, TableContent_1);
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Download, "test_task_id", params, "test_session_id");

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);

        tableDataServicePtr->Put(key, TableContent_1);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), TableContent_1);
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        std::vector<TTableRange> ranges = {{"test_part_id"}};
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id", .TableRanges = ranges};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        // No table in tableDataServicePtr
        // auto key = GetTableDataServiceKey(input.TableId, "test_part_id", 0);
        // tableDataServicePtr->Put(key, ytTableContent);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        UNIT_ASSERT(ytUploadedTablesMock->IsEmpty());
    }

    Y_UNIT_TEST(RunMergeTask) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
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
        tableDataServicePtr->Put(key_1, TableContent_1);
        ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(key_3, TableContent_3);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT_C(resultTableContent, "Result table content is empty");
        UNIT_ASSERT_NO_DIFF(*resultTableContent, TableContent_1 + TableContent_2 + TableContent_3);
    }

    Y_UNIT_TEST(RunMergeTaskWithNoTable) {

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
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
        tableDataServicePtr->Put(key_1, TableContent_1);
        // No table in Yt
        // ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(key_3, TableContent_3);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        auto resultTableContent = tableDataServicePtr->Get(tableDataServiceExpectedOutputKey).GetValueSync();
        UNIT_ASSERT(!resultTableContent);
    }
}

} // namespace NYql
