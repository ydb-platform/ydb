#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>

#include <ydb/apps/ydbd/export/export.h>
#include <ydb/library/testlib/parquet_helpers/parquet_helpers.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

namespace NKikimr {

namespace {

using EDataFormat = NDataShard::NBackupRestoreTraits::EDataFormat;
using ECompressionCodec = NDataShard::NBackupRestoreTraits::ECompressionCodec;

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

TVector<std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
    return { { "key", NScheme::TTypeInfo(NScheme::NTypeIds::String) }, { "value", NScheme::TTypeInfo(NScheme::NTypeIds::String) } };
}

TIntrusiveConstPtr<NTable::TRowScheme> MakeSchema() {
    NTable::TScheme::TTableSchema tableSchema;
    tableSchema.Columns[0] = NTable::TColumn("key", 0, NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
    tableSchema.Columns[0].KeyOrder = 0;

    tableSchema.Columns[1] = NTable::TColumn("value", 1, NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
    tableSchema.Columns[1].KeyOrder = 1;

    return NTable::TRowScheme::Make(tableSchema.Columns, NUtil::TSecond());
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);
    return columns;
}

NKikimrSchemeOp::TBackupTask MakeBackupTask(const TString& bucketName, EDataFormat dataFormat = EDataFormat::YdbDump)
{
    NKikimrSchemeOp::TBackupTask backupTask;
    backupTask.SetEnablePermissions(true);
    auto& s3Settings = *backupTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    switch (dataFormat) {
        case EDataFormat::YdbDump:
            s3Settings.MutableExportDataSettings()->MutableYdbDump();
            break;
        case EDataFormat::Parquet:
            s3Settings.MutableExportDataSettings()->MutableParquet();
            break;
        case EDataFormat::Invalid:
            Y_ABORT("Invalid data format");
    }

    auto& table = *backupTask.MutableTable();
    auto& tableDescription = *table.MutableColumnTableDescription();
    tableDescription.SetColumnShardCount(4);
    auto& col1 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col1.SetName("key");
    col1.SetType("String");

    auto& col2 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col2.SetName("value");
    col2.SetType("String");
    table.MutableSelf();
    return backupTask;
}

void EnableDataFormat(TTestActorRuntime& runtime, EDataFormat dataFormat) {
    if (dataFormat == EDataFormat::Parquet) {
        runtime.GetAppData().FeatureFlags.SetEnableExportInParquet(true);
    }
}

void AssertParquetData(const TString& data, const TVector<std::pair<TString, TString>>& expectedRows)
{
    UNIT_ASSERT_GE(data.size(), 8u);
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(data.data(), 4), "PAR1");
    UNIT_ASSERT_VALUES_EQUAL(TStringBuf(data.data() + data.size() - 4, 4), "PAR1");

    const auto table = NTestUtils::ReadParquet(data);
    UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), expectedRows.size());
    UNIT_ASSERT_VALUES_EQUAL(table->num_columns(), 2);

    const auto keyColumn = table->GetColumnByName("key");
    const auto valueColumn = table->GetColumnByName("value");
    UNIT_ASSERT(keyColumn);
    UNIT_ASSERT(valueColumn);
    UNIT_ASSERT_VALUES_EQUAL(keyColumn->num_chunks(), 1);
    UNIT_ASSERT_VALUES_EQUAL(valueColumn->num_chunks(), 1);

    const auto keys = std::static_pointer_cast<arrow::BinaryArray>(keyColumn->chunk(0));
    const auto values = std::static_pointer_cast<arrow::BinaryArray>(valueColumn->chunk(0));
    for (size_t i = 0; i < expectedRows.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(keys->GetString(i), expectedRows[i].first);
        UNIT_ASSERT_VALUES_EQUAL(values->GetString(i), expectedRows[i].second);
    }
}

void AssertExportedData(const TString& bucketName, EDataFormat dataFormat, Aws::S3::S3Client& s3Client, const TString& expectedCsv,
    const TVector<std::pair<TString, TString>>& expectedRows)
{
    const TString dataKey = NDataShard::NBackupRestoreTraits::DataKeySuffix(0, dataFormat, ECompressionCodec::None, false);
    const auto data = NTestUtils::GetObject(bucketName, dataKey, s3Client);

    switch (dataFormat) {
        case EDataFormat::YdbDump:
            UNIT_ASSERT_VALUES_EQUAL(dataKey, "data_00.csv");
            UNIT_ASSERT_VALUES_EQUAL(data, expectedCsv);
            break;
        case EDataFormat::Parquet:
            UNIT_ASSERT_VALUES_EQUAL(dataKey, "data_00.parquet");
            AssertParquetData(data, expectedRows);
            break;
        case EDataFormat::Invalid:
            UNIT_FAIL("Invalid data format");
    }
}

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;

class TGrabActor: public TActorBootstrapped<TGrabActor> {
    std::deque<NThreading::TPromise<TAutoPtr<IEventHandle>>> Futures;
    std::deque<TAutoPtr<IEventHandle>> Inputs;
    TMutex Mutex;
    std::unique_ptr<NColumnShard::NBackup::TExportDriver> Driver;

    // non-owning pointer to the exporter actor
    NTable::IScan* Exporter = nullptr;

    const TString BucketName;
    const EDataFormat DataFormat;

public:
    // Non-owning pointer to the actor runtime
    TTestActorRuntime* Runtime;

    TGrabActor(TTestActorRuntime* runtime, TString bucketName, EDataFormat dataFormat)
        : BucketName(std::move(bucketName))
        , DataFormat(dataFormat)
        , Runtime(runtime)
    {
    }

    void Bootstrap() {
        NDataShard::IExport::TTableColumns columns;
        columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
        columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);

        auto exportFactory = std::make_shared<TDataShardExportFactory>();

        std::unique_ptr<NTable::IScan> exporter =
            NColumnShard::NBackup::CreateIScanExportUploader(SelfId(), MakeBackupTask(BucketName, DataFormat), exportFactory.get(), columns, 0)
                .DetachResult();
        UNIT_ASSERT(exporter);
        Exporter = exporter.get();
        Driver = std::make_unique<NColumnShard::NBackup::TExportDriver>(TActorContext::ActorSystem(), SelfId());
        auto initialState = Exporter->Prepare(Driver.get(), MakeSchema());
        UNIT_ASSERT_VALUES_EQUAL(initialState.Scan, NTable::EScan::Feed);
        Y_UNUSED(exporter.release());

        NTable::TLead lead;
        auto seekState = Exporter->Seek(lead, 0);
        UNIT_ASSERT_VALUES_EQUAL(seekState, NTable::EScan::Feed);

        Become(&TGrabActor::StateFunc);
    }

    void SendData() {
        auto recordBatch = TestRecordBatch();
        TVector<TSerializedCellVec> cellVec = NColumnShard::NBackup::BatchToRows(recordBatch, MakeYdbSchema()).DetachResult();
        for (const auto& row : cellVec) {
            NTable::TRowState rowState(row.GetCells().size());
            int i = 0;
            for (const auto& cell : row.GetCells()) {
                rowState.Set(i++, { NTable::ECellOp::Set, NTable::ELargeObj::Inline }, cell);
            }
            Exporter->Feed({}, rowState);
        }
        auto exhaustedState = Exporter->Exhausted();
        UNIT_ASSERT_VALUES_EQUAL(exhaustedState, NTable::EScan::Sleep);
    }

    void Handle(NColumnShard::TEvPrivate::TEvBackupExportState::TPtr& ev) {
        if (ev->Get()->State == NTable::EScan::Final) {
            return;
        }
        SendData();
    }

    STFUNC(StateFunc) {
        if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvBackupExportState::EventType) {
            NColumnShard::TEvPrivate::TEvBackupExportState::TPtr* x =
                reinterpret_cast<NColumnShard::TEvPrivate::TEvBackupExportState::TPtr*>(&ev);
            Handle(*x);
        }

        TGuard<TMutex> lock(Mutex);
        if (!Futures.empty()) {
            auto front = Futures.front();
            Futures.pop_front();
            front.SetValue(ev);
            return;
        }
        Inputs.push_back(ev);
    }

    NThreading::TFuture<TAutoPtr<IEventHandle>> WaitRequest() {
        TGuard<TMutex> lock(Mutex);
        if (!Inputs.empty()) {
            auto front = Inputs.front();
            Inputs.pop_front();
            return NThreading::MakeFuture(front);
        }
        Futures.push_back(NThreading::NewPromise<TAutoPtr<IEventHandle>>());
        return Futures.back();
    }

    TAutoPtr<IEventHandle> GetRequest() {
        auto future = WaitRequest();
        while (!future.HasValue()) {
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        }
        return future.GetValue();
    }
};

}   // namespace

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(IScan) {
    void AssertExportObjects(const TString& bucketName, EDataFormat dataFormat, Aws::S3::S3Client& s3Client, const TString& expectedCsv,
        const TVector<std::pair<TString, TString>>& expectedRows)
    {
        const TString dataKey = NDataShard::NBackupRestoreTraits::DataKeySuffix(0, dataFormat, ECompressionCodec::None, false);
        std::vector<TString> result = NTestUtils::GetObjectKeys(bucketName, s3Client);
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount(bucketName, s3Client), 0);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", result), TStringBuilder() << dataKey << ",metadata.json,permissions.pb,scheme.pb");

        const auto scheme = NTestUtils::GetObject(bucketName, "scheme.pb", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(scheme,
            "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\ncolumns "
            "{\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  "
            "}\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");

        const auto metadata = NTestUtils::GetObject(bucketName, "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(
            metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[],\"indexes\":[]}");

        AssertExportedData(bucketName, dataFormat, s3Client, expectedCsv, expectedRows);
    }

    void TestSimpleExport(EDataFormat dataFormat, const TString& bucketName) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        EnableDataFormat(*runtime, dataFormat);

        auto grabActor = new TGrabActor(runtime.get(), bucketName, dataFormat);
        runtime->Register(grabActor);

        while (true) {
            auto request = grabActor->GetRequest();
            auto event = request->Get<NColumnShard::TEvPrivate::TEvBackupExportState>();
            UNIT_ASSERT_C(event, request->GetTypeName());
            if (event->State == NTable::EScan::Final) {
                break;
            }
        }

        AssertExportObjects(bucketName, dataFormat, s3Client, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n",
            { { "foo", "one" }, { "bar", "two" }, { "baz", "three" } });
    }

    Y_UNIT_TEST(SimpleExportCsv) {
        TestSimpleExport(EDataFormat::YdbDump, "iscan-simple-csv");
    }

    Y_UNIT_TEST(SimpleExportParquet) {
        TestSimpleExport(EDataFormat::Parquet, "iscan-simple-parquet");
    }

    void TestUploaderExport(EDataFormat dataFormat, const TString& bucketName) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        EnableDataFormat(*runtime, dataFormat);

        const auto edge = runtime->AllocateEdgeActor(0);
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask(bucketName, dataFormat), exportFactory.get(), MakeYdbColumns(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), true)));
        auto event = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(event->IsFinish);

        runtime->DispatchEvents({}, TDuration::Seconds(5));
        AssertExportObjects(bucketName, dataFormat, s3Client, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n",
            { { "foo", "one" }, { "bar", "two" }, { "baz", "three" } });
    }

    Y_UNIT_TEST(UploaderExportCsv) {
        TestUploaderExport(EDataFormat::YdbDump, "iscan-uploader-csv");
    }

    Y_UNIT_TEST(UploaderExportParquet) {
        TestUploaderExport(EDataFormat::Parquet, "iscan-uploader-parquet");
    }

    void TestMultiExport(EDataFormat dataFormat, const TString& bucketName) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        EnableDataFormat(*runtime, dataFormat);

        const auto edge = runtime->AllocateEdgeActor(0);
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask(bucketName, dataFormat), exportFactory.get(), MakeYdbColumns(), 0);
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
        AssertExportObjects(bucketName, dataFormat, s3Client,
            "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n",
            {
                { "foo", "one" },
                { "bar", "two" },
                { "baz", "three" },
                { "foo", "one" },
                { "bar", "two" },
                { "baz", "three" },
            });
    }

    Y_UNIT_TEST(MultiExportCsv) {
        TestMultiExport(EDataFormat::YdbDump, "iscan-multi-csv");
    }

    Y_UNIT_TEST(MultiExportParquet) {
        TestMultiExport(EDataFormat::Parquet, "iscan-multi-parquet");
    }

    Y_UNIT_TEST(ShouldRejectParquetExportWithEncryption) {
        TRuntimePtr runtime(new TTestBasicRuntime());
        SetupTabletServices(*runtime);
        runtime->GetAppData().FeatureFlags.SetEnableExportInParquet(true);

        NDataShard::IExport::TTableColumns columns;
        columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
        columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);

        auto backupTask = MakeBackupTask("test");
        backupTask.MutableS3Settings()->MutableExportDataSettings()->MutableParquet();
        backupTask.MutableEncryptionSettings()->SetEncryptionAlgorithm("AES-256-GCM");

        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto result = runtime->RunCall([&] {
            return NColumnShard::NBackup::CreateIScanExportUploader(runtime->AllocateEdgeActor(), backupTask, exportFactory.get(), columns, 0);
        });

        UNIT_ASSERT(!result);
        UNIT_ASSERT_STRING_CONTAINS(result.GetErrorMessage(), "Encryption is not supported for parquet files");
    }
}

}   // namespace NKikimr
