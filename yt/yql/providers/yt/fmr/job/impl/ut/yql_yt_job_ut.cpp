#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_mock.h>

namespace NYql::NFmr {

TString TableContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

Y_UNIT_TEST_SUITE(FmrJobTests) {
    Y_UNIT_TEST(DownloadTable) {
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId = "test_table_id"};
        auto params = TDownloadTaskParams(input, output);

        ytUploadedTablesMock->AddTable(input, TableContent);

        auto err = job->Download(params);

        UNIT_ASSERT_C(!err,err.GetRef());
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get("test_table_id").GetValueSync().GetRef(), TableContent);
    }

    Y_UNIT_TEST(UploadTable) {
        TString ytTableContent = TableContent;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TYtTableRef output = TYtTableRef("test_cluster", "test_path");
        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id"};
        auto params = TUploadTaskParams(input, output);

        tableDataServicePtr->Put(input.TableId, ytTableContent);

        auto err = job->Upload(params);

        UNIT_ASSERT_C(!err,err.GetRef()); // TODO - исправить это
        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), ytTableContent);
    }

    Y_UNIT_TEST(MergeFmrTables) {
        TString TableContent_1 =
        "{\"key\"=\"1\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"2\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"3\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"4\";\"subkey\"=\"4\";\"value\"=\"qzz\"};)";
        TString TableContent_2 =
        "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
        TString TableContent_3 =
        "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1"};
        TFmrTableInputRef input_2 = TFmrTableInputRef{.TableId = "test_table_id_2"};
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3"};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId= "test_table_id_output"};
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);

        tableDataServicePtr->Put(input_1.TableId, TableContent_1);
        tableDataServicePtr->Put(input_2.TableId, TableContent_2);
        tableDataServicePtr->Put(input_3.TableId, TableContent_3);

        auto err = job->Merge(params);

        UNIT_ASSERT_C(!err,err.GetRef());
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get(output.TableId).GetValueSync().GetRef(), TableContent_1 + TableContent_2 + TableContent_3);
    }

    Y_UNIT_TEST(MergeMixedTables) {
        TString TableContent_1 =
        "{\"key\"=\"1\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"2\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"3\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"4\";\"subkey\"=\"4\";\"value\"=\"qzz\"};)";
        TString TableContent_2 =
        "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
        TString TableContent_3 =
        "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1"};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3"};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId= "test_table_id_output"};
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);

        tableDataServicePtr->Put(input_1.TableId, TableContent_1);
        ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(input_3.TableId, TableContent_3);

        auto err = job->Merge(params);

        UNIT_ASSERT_C(!err,err.GetRef());
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get(output.TableId).GetValueSync().GetRef(), TableContent_1 + TableContent_2 + TableContent_3);
    }
}

Y_UNIT_TEST_SUITE(TaskRunTests) {
    Y_UNIT_TEST(RunDownloadTask) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId = "test_table_id"};

        ytUploadedTablesMock->AddTable(input, ytTableContent);
        TDownloadTaskParams params = TDownloadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Download, "test_task_id", params, "test_session_id");


        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get("test_table_id").GetValueSync().GetRef(), ytTableContent);
    }

    Y_UNIT_TEST(RunUploadTask) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id"};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        tableDataServicePtr->Put(input.TableId, ytTableContent);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), ytTableContent);
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableInputRef input = TFmrTableInputRef{.TableId = "test_table_id"};
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);
        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        // No table in tableDataServicePtr
        // tableDataServicePtr->Put(input.TableId, ytTableContent);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        UNIT_ASSERT(ytUploadedTablesMock->IsEmpty());
    }

    Y_UNIT_TEST(RunMergeTask) {
        TString TableContent_1 =
        "{\"key\"=\"1\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"2\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"3\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"4\";\"subkey\"=\"4\";\"value\"=\"qzz\"};)";
        TString TableContent_2 =
        "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
        TString TableContent_3 =
        "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1"};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3"};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId= "test_table_id_output"};
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);

        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        tableDataServicePtr->Put(input_1.TableId, TableContent_1);
        ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(input_3.TableId, TableContent_3);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get(
            output.TableId).GetValueSync().GetRef(),
            TableContent_1 + TableContent_2 + TableContent_3
        );
    }

    Y_UNIT_TEST(RunMergeTaskWithNoTable) {
        TString TableContent_1 =
        "{\"key\"=\"1\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"2\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"3\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"4\";\"subkey\"=\"4\";\"value\"=\"qzz\"};)";
        TString TableContent_2 =
        "{\"key\"=\"5\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"6\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"7\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"8\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";
        TString TableContent_3 =
        "{\"key\"=\"9\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"10\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"11\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"12\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableInputRef input_1 = TFmrTableInputRef{.TableId = "test_table_id_1"};
        TYtTableRef input_2 = TYtTableRef("test_path", "test_cluster");
        TFmrTableInputRef input_3 = TFmrTableInputRef{.TableId = "test_table_id_3"};
        TTaskTableRef input_table_ref_1 = {input_1};
        TTaskTableRef input_table_ref_2 = {input_2};
        TTaskTableRef input_table_ref_3 = {input_3};
        TFmrTableOutputRef output = TFmrTableOutputRef{.TableId= "test_table_id_output"};
        std::vector<TTaskTableRef> inputs = {input_table_ref_1, input_table_ref_2, input_table_ref_3};
        auto params = TMergeTaskParams(inputs, output);

        TTask::TPtr task = MakeTask(ETaskType::Upload, "test_task_id", params, "test_session_id");

        tableDataServicePtr->Put(input_1.TableId, TableContent_1);
        // No table in Yt
        // ytUploadedTablesMock->AddTable(input_2, TableContent_2);
        tableDataServicePtr->Put(input_3.TableId, TableContent_3);

        ETaskStatus status = RunJob(task, tableDataServicePtr, ytService, cancelFlag).TaskStatus;
        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        UNIT_ASSERT(!tableDataServicePtr->Get(output.TableId).GetValueSync());
    }
}

} // namespace NYql
