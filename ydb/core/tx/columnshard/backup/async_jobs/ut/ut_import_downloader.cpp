#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/backup/async_jobs/import_downloader.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/import_common.h>

#include <ydb/apps/ydbd/export/export.h>
#include <ydb/library/testlib/backup_test_enums/backup_test_enums.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <aws/s3/model/HeadObjectRequest.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <contrib/libs/zlib/zlib.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {

namespace {

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;
using EDataFormat = NDataShard::NBackupRestoreTraits::EDataFormat;

TStringBuf FormatName(EDataFormat format) {
    switch (format) {
        case EDataFormat::YdbDump:
            return "csv";
        case EDataFormat::Parquet:
            return "parquet";
        case EDataFormat::Invalid:
            break;
    }
    Y_ABORT("Unexpected data format");
    return {};
}

void SetBackupFormat(NKikimrSchemeOp::TBackupTask& backupTask, EDataFormat format) {
    auto& settings = *backupTask.MutableS3Settings()->MutableExportDataSettings();
    switch (format) {
        case EDataFormat::YdbDump:
            settings.MutableYdbDump();
            return;
        case EDataFormat::Parquet:
            settings.MutableParquet();
            return;
        case EDataFormat::Invalid:
            break;
    }
    Y_ABORT("Unexpected data format");
}

std::shared_ptr<arrow::RecordBatch> TestRecordBatch() {
    std::vector<std::string> keys = { "foo", "bar", "baz" };
    std::vector<std::string> values = { "one", "two", "three" };

    arrow::StringBuilder key_builder;
    for (const auto& k : keys) {
        Y_UNUSED(key_builder.Append(k));
    }
    std::shared_ptr<arrow::Array> key_array;
    Y_UNUSED(key_builder.Finish(&key_array));

    arrow::StringBuilder value_builder;
    for (const auto& v : values) {
        Y_UNUSED(value_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> value_array;
    Y_UNUSED(value_builder.Finish(&value_array));

    auto schema = arrow::schema({ arrow::field("key", arrow::binary()), arrow::field("value", arrow::binary()) });

    return arrow::RecordBatch::Make(schema, keys.size(), { key_array, value_array });
}

// 3-column record batch: key, col_a, col_b
std::shared_ptr<arrow::RecordBatch> TestRecordBatch3Cols() {
    std::vector<std::string> keys = { "k1", "k2" };
    std::vector<std::string> colA = { "a1", "a2" };
    std::vector<std::string> colB = { "b1", "b2" };

    arrow::StringBuilder key_builder;
    for (const auto& k : keys) {
        Y_UNUSED(key_builder.Append(k));
    }
    std::shared_ptr<arrow::Array> key_array;
    Y_UNUSED(key_builder.Finish(&key_array));

    arrow::StringBuilder a_builder;
    for (const auto& v : colA) {
        Y_UNUSED(a_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> a_array;
    Y_UNUSED(a_builder.Finish(&a_array));

    arrow::StringBuilder b_builder;
    for (const auto& v : colB) {
        Y_UNUSED(b_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> b_array;
    Y_UNUSED(b_builder.Finish(&b_array));

    auto schema = arrow::schema({
        arrow::field("key", arrow::binary()), arrow::field("col_a", arrow::binary()), arrow::field("col_b", arrow::binary()),
    });

    return arrow::RecordBatch::Make(schema, keys.size(), { key_array, a_array, b_array });
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);
    return columns;
}

NDataShard::IExport::TTableColumns MakeYdbColumns3Cols() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "col_a", false);
    columns[2] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "col_b", false);
    return columns;
}

THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
    return {
        { 1, { "key", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 2, { "value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
    };
}

// YdbSchema with deliberately reversed value column order: key, col_b, col_a.
// This simulates the scenario where YdbSchema order differs from RowScheme order.
// The fix ensures RowScheme order (key, col_a, col_b) is used, not YdbSchema order.
THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema3ColsReversed() {
    return {
        { 1, { "key", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 3, { "col_b", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 2, { "col_a", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
    };
}

NKikimrSchemeOp::TBackupTask MakeBackupTask(const TString& bucketName, EDataFormat format) {
    NKikimrSchemeOp::TBackupTask backupTask;
    backupTask.SetEnablePermissions(true);
    auto& s3Settings = *backupTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    SetBackupFormat(backupTask, format);
    auto& table = *backupTask.MutableTable();
    auto& tableDescription = *table.MutableColumnTableDescription();
    tableDescription.SetColumnShardCount(4);
    auto& col1 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col1.SetName("key");
    col1.SetType("Utf8");

    auto& col2 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col2.SetName("value");
    col2.SetType("Utf8");
    table.MutableSelf();
    return backupTask;
}

NKikimrSchemeOp::TBackupTask MakeBackupTask3Cols(const TString& bucketName, EDataFormat format) {
    NKikimrSchemeOp::TBackupTask backupTask;
    backupTask.SetEnablePermissions(true);
    auto& s3Settings = *backupTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    SetBackupFormat(backupTask, format);
    auto& table = *backupTask.MutableTable();
    auto& tableDescription = *table.MutableColumnTableDescription();
    tableDescription.SetColumnShardCount(4);
    auto& col1 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col1.SetName("key");
    col1.SetType("Utf8");

    auto& col2 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col2.SetName("col_a");
    col2.SetType("Utf8");

    auto& col3 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col3.SetName("col_b");
    col3.SetType("Utf8");
    table.MutableSelf();
    return backupTask;
}

NKikimrSchemeOp::TRestoreTask MakeRestoreTask(const TString& bucketName) {
    NKikimrSchemeOp::TRestoreTask restoreTask;
    auto& s3Settings = *restoreTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    auto& description = *restoreTask.MutableTableDescription();
    auto& col1 = *description.AddColumns();
    col1.SetName("key");
    col1.SetType("Utf8");
    col1.SetId(1);
    col1.SetTypeId(NScheme::NTypeIds::Utf8);
    auto& col2 = *description.AddColumns();
    col2.SetName("value");
    col2.SetType("Utf8");
    col2.SetId(2);
    col2.SetTypeId(NScheme::NTypeIds::Utf8);
    description.AddKeyColumnNames("key");
    description.AddKeyColumnIds(1);
    return restoreTask;
}

// 3-column restore task: key (id=1), col_a (id=2), col_b (id=3)
NKikimrSchemeOp::TRestoreTask MakeRestoreTask3Cols(const TString& bucketName) {
    NKikimrSchemeOp::TRestoreTask restoreTask;
    auto& s3Settings = *restoreTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    auto& description = *restoreTask.MutableTableDescription();
    auto& col1 = *description.AddColumns();
    col1.SetName("key");
    col1.SetType("Utf8");
    col1.SetId(1);
    col1.SetTypeId(NScheme::NTypeIds::Utf8);
    auto& col2 = *description.AddColumns();
    col2.SetName("col_a");
    col2.SetType("Utf8");
    col2.SetId(2);
    col2.SetTypeId(NScheme::NTypeIds::Utf8);
    auto& col3 = *description.AddColumns();
    col3.SetName("col_b");
    col3.SetType("Utf8");
    col3.SetId(3);
    col3.SetTypeId(NScheme::NTypeIds::Utf8);
    description.AddKeyColumnNames("key");
    description.AddKeyColumnIds(1);
    return restoreTask;
}

TString MakeYtGzip(TStringBuf text) {
    const auto rows = NYT::NodeFromYsonString(text, NYson::EYsonType::ListFragment);
    const auto wrapped = NYT::NodeToYsonString(rows, NYson::EYsonFormat::Binary);
    const auto input = TStringBuf(wrapped).SubStr(1, wrapped.size() - 2);
    z_stream stream{};
    UNIT_ASSERT_VALUES_EQUAL(deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16 + MAX_WBITS, 8, Z_DEFAULT_STRATEGY), Z_OK);
    TString output;
    output.resize(deflateBound(&stream, input.size()));
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
    stream.avail_in = input.size();
    stream.next_out = reinterpret_cast<Bytef*>(output.Detach());
    stream.avail_out = output.size();
    UNIT_ASSERT_VALUES_EQUAL(deflate(&stream, Z_FINISH), Z_STREAM_END);
    output.resize(stream.total_out);
    deflateEnd(&stream);
    return output;
}

NKikimrSchemeOp::TRestoreTask MakeYtRestoreTask(const TString& bucket) {
    auto task = MakeRestoreTask(bucket);
    task.MutableTableDescription()->MutableColumns(0)->SetNotNull(true);
    task.SetYtSource(true);
    task.MutableYtImportPlan()->SetVersion(1);
    return task;
}

void AddYtPart(
    NKikimrSchemeOp::TRestoreTask& task, Aws::S3::S3Client& client, TStringBuf key, TStringBuf firstKey, TStringBuf text, ui64 start, ui64 end) {
    const auto data = MakeYtGzip(text);
    NTestUtils::UploadObject(task.GetS3Settings().GetBucket(), TString(key), data, client);
    const auto head = client.HeadObject(Aws::S3::Model::HeadObjectRequest().WithBucket(task.GetS3Settings().GetBucket()).WithKey(TString(key)));
    UNIT_ASSERT_C(head.IsSuccess(), head.GetError().GetMessage());
    auto* part = task.MutableYtImportPlan()->AddParts();
    part->SetObjectKey(TString(key));
    const auto& etag = head.GetResult().GetETag();
    part->SetETag(etag.data(), etag.size());
    part->SetContentLength(data.size());
    part->SetStartRow(start);
    part->SetEndRow(end);
    const TVector<TCell> cells{ TCell(firstKey.data(), firstKey.size()) };
    part->SetFirstKey(TSerializedCellVec(cells).GetBuffer());
}

TActorId StartYtWrapper(TTestActorRuntime& runtime, const TActorId& edge, const NKikimrSchemeOp::TRestoreTask& task) {
    auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), task.GetTableDescription(), ui32(0));
    auto actor = NColumnShard::NBackup::CreateImportDownloader(edge, 0, task, NDataShard::TTableInfo(0, userTable), MakeYdbSchema());
    return runtime.Register(actor.release());
}

}   // namespace

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(AsyncJobs) {
    Y_UNIT_TEST(YtMultipartAndWholeSourceReplay) {
        const TString bucket = "column-yt-multipart-replay";
        auto client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucket, client);
        auto task = MakeYtRestoreTask(bucket);
        AddYtPart(task, client, "part_0_1", "k1", "{key=k1;value=one};{key=k2;value=two};", 0, 1);
        AddYtPart(task, client, "part_2_2", "k3", "{key=k3;value=three};", 2, 2);
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        // YT must keep using row uploads even when native direct-part mode is on.
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(true);
        const auto edge = runtime.AllocateEdgeActor();
        auto actor = StartYtWrapper(runtime, edge, task);
        TAutoPtr<IEventHandle> handle;
        auto first = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!first->IsLast);
        UNIT_ASSERT_VALUES_EQUAL(first->Data->num_rows(), 2);
        runtime.Send(new IEventHandle(actor, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));
        auto second = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!second->IsLast);
        UNIT_ASSERT_VALUES_EQUAL(second->Data->num_rows(), 1);
        // Lose the wrapper after the first part was acknowledged. Its restart
        // deliberately replays all objects, matching CS OPERATION_REPLACE recovery.
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPoisonPill()));
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        actor = StartYtWrapper(runtime, edge, task);
        first = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT_VALUES_EQUAL(first->Data->num_rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::static_pointer_cast<arrow::StringArray>(first->Data->column(0))->GetString(0), "k1");
        runtime.Send(new IEventHandle(actor, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));
        second = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT_VALUES_EQUAL(second->Data->num_rows(), 1);
        runtime.Send(new IEventHandle(actor, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));
        auto final = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(final->IsLast);
        UNIT_ASSERT(!final->Error);
        UNIT_ASSERT(!final->Data);
    }

    Y_UNIT_TEST(YtEmptyPlanDoesNotDownload) {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        const auto edge = runtime.AllocateEdgeActor();
        StartYtWrapper(runtime, edge, MakeYtRestoreTask("bucket-does-not-exist"));
        TAutoPtr<IEventHandle> handle;
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(result->IsLast);
        UNIT_ASSERT(!result->Error);
        UNIT_ASSERT(!result->Data);
    }

    Y_UNIT_TEST(YtRejectsOverlappingPartTail) {
        const TString bucket = "column-yt-overlap";
        auto client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucket, client);
        auto task = MakeYtRestoreTask(bucket);
        AddYtPart(task, client, "part_0_1", "k1", "{key=k1;value=one};{key=k4;value=four};", 0, 1);
        AddYtPart(task, client, "part_2_2", "k3", "{key=k3;value=three};", 2, 2);
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        const auto edge = runtime.AllocateEdgeActor();
        StartYtWrapper(runtime, edge, task);
        TAutoPtr<IEventHandle> handle;
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(result->IsLast);
        UNIT_ASSERT_STRING_CONTAINS(result->Error, "key is out of range");
    }

    void Import(EDataFormat format) {
        const TString bucketName = TStringBuilder() << "test-import-" << FormatName(format);
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        runtime->GetAppData().FeatureFlags.SetEnableExportInParquet(true);
        runtime->GetAppData().FeatureFlags.SetEnableImportInParquet(true);

        const auto edge = runtime->AllocateEdgeActor(0);
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask(bucketName, format), exportFactory.get(), MakeYdbColumns(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), false)));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), true)));
        auto event1 = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(!event1->IsFinish);
        auto event2 = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(event2->IsFinish);

        runtime->DispatchEvents({}, TDuration::Seconds(5));
        std::vector<TString> result = NTestUtils::GetObjectKeys(bucketName, s3Client);
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount(bucketName, s3Client), 0);
        const TString dataKey = format == EDataFormat::Parquet ? "data_00.parquet" : "data_00.csv";
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", result), TStringBuilder() << dataKey << ",metadata.json,permissions.pb,scheme.pb");
        auto scheme = NTestUtils::GetObject(bucketName, "scheme.pb", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(scheme,
            "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  }\n}\ncolumns "
            "{\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  "
            "}\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject(bucketName, "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(
            metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[],\"indexes\":[]}");
        auto data = NTestUtils::GetObject(bucketName, dataKey, s3Client);
        if (format == EDataFormat::Parquet) {
            UNIT_ASSERT_GE(data.size(), 8);
            UNIT_ASSERT(data.StartsWith("PAR1"));
            UNIT_ASSERT(data.EndsWith("PAR1"));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");
        }

        auto restoreTask = MakeRestoreTask(bucketName);
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));

        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
            edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, MakeYdbSchema());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        auto event3 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!event3->IsLast);
        UNIT_ASSERT_VALUES_EQUAL(event3->Data->ToString(),
            "key:   [\n    \"foo\",\n    \"bar\",\n    \"baz\",\n    \"foo\",\n    \"bar\",\n    \"baz\"\n  ]\nvalue:   [\n    \"one\",\n    "
            "\"two\",\n    \"three\",\n    \"one\",\n    \"two\",\n    \"three\"\n  ]\n");
        runtime->Send(new IEventHandle(importActorId, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));

        auto event4 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(event4->IsLast);
        UNIT_ASSERT(!event4->Data);
    }

    Y_UNIT_TEST(Import, EBackupTestDataFormat) {
        Import(ToDataFormat(Arg<0>()));
    }

    void ImportSchemaOrder(EDataFormat format) {
        const TString bucketName = TStringBuilder() << "test-schema-order-" << FormatName(format);
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        runtime->GetAppData().FeatureFlags.SetEnableExportInParquet(true);
        runtime->GetAppData().FeatureFlags.SetEnableImportInParquet(true);

        const auto edge = runtime->AllocateEdgeActor(0);

        // Export 3-column data (key, col_a, col_b) to S3
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask3Cols(bucketName, format), exportFactory.get(), MakeYdbColumns3Cols(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch3Cols(), true)));
        auto exportEvent = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(exportEvent->IsFinish);

        runtime->DispatchEvents({}, TDuration::Seconds(5));

        const TString dataKey = format == EDataFormat::Parquet ? "data_00.parquet" : "data_00.csv";
        const auto keys = NTestUtils::GetObjectKeys(bucketName, s3Client);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", keys), TStringBuilder() << dataKey << ",metadata.json,permissions.pb,scheme.pb");
        const auto data = NTestUtils::GetObject(bucketName, dataKey, s3Client);
        if (format == EDataFormat::Parquet) {
            UNIT_ASSERT_GE(data.size(), 8);
            UNIT_ASSERT(data.StartsWith("PAR1"));
            UNIT_ASSERT(data.EndsWith("PAR1"));
        }

        auto restoreTask = MakeRestoreTask3Cols(bucketName);
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));

        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
            edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, MakeYdbSchema3ColsReversed());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        auto event3 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!event3->IsLast);
        UNIT_ASSERT(event3->Data);

        auto batch = event3->Data;
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 3);
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(0)->name(), "key");
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(1)->name(), "col_a");
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(2)->name(), "col_b");

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), "key:   [\n    \"k1\",\n    \"k2\"\n  ]\n"
                                                    "col_a:   [\n    \"a1\",\n    \"a2\"\n  ]\n"
                                                    "col_b:   [\n    \"b1\",\n    \"b2\"\n  ]\n");

        runtime->Send(new IEventHandle(importActorId, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));

        auto event4 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(event4->IsLast);
        UNIT_ASSERT(!event4->Data);
    }

    Y_UNIT_TEST(ImportSchemaOrder, EBackupTestDataFormat) {
        ImportSchemaOrder(ToDataFormat(Arg<0>()));
    }

    // Nothing is stored for a column table: the downloader gets back what it asked to store.
    Y_UNIT_TEST(ImportKeepsTheCheckpointOfTheDownloader) {
        const TString bucketName = "test-import-checkpoint";
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        SetupTabletServices(*runtime);
        runtime->GetAppData().FeatureFlags.SetEnableExportInParquet(true);
        runtime->GetAppData().FeatureFlags.SetEnableImportInParquet(true);

        const auto edge = runtime->AllocateEdgeActor(0);

        // a backup for the downloader to work on
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask(bucketName, EDataFormat::Parquet), exportFactory.get(), MakeYdbColumns(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), true)));
        auto exported = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(exported->IsFinish);
        runtime->DispatchEvents({}, TDuration::Seconds(5));

        auto restoreTask = MakeRestoreTask(bucketName);
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));
        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
            edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, MakeYdbSchema());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        NDataShard::TS3Download info;
        info.DataETag = "etag";
        info.DownloadState.MutableParquet()->SetCommittedRowGroups(1);
        info.DownloadState.MutableParquet()->SetChecksumOffset(100);
        info.DownloadState.SetEncryptedDeserializerState("the state of a decryption");
        runtime->Send(new IEventHandle(importActorId, edge, new TEvDataShard::TEvStoreS3DownloadInfo(0, info)));

        const auto stored = runtime->GrabEdgeEvent<TEvDataShard::TEvS3DownloadInfo>(handle);
        UNIT_ASSERT(stored->Info.DataETag);
        UNIT_ASSERT_VALUES_EQUAL(*stored->Info.DataETag, "etag");
        UNIT_ASSERT_VALUES_EQUAL(stored->Info.DownloadState.GetParquet().GetCommittedRowGroups(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stored->Info.DownloadState.GetParquet().GetChecksumOffset(), 100);
        UNIT_ASSERT_VALUES_EQUAL(stored->Info.DownloadState.GetEncryptedDeserializerState(), "the state of a decryption");
    }
}

}   // namespace NKikimr
