#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/core/tx/datashard/export_scan.h>
#include <ydb/core/wrappers/abstract.h>

#include <ydb/apps/ydbd/export/export.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include <util/system/env.h>

#include <iterator>
#include <type_traits>

namespace NKikimr {
namespace {

using namespace NActors;
using namespace NColumnShard;
using EDataFormat = NDataShard::NBackupRestoreTraits::EDataFormat;
using ECompressionCodec = NDataShard::NBackupRestoreTraits::ECompressionCodec;

Aws::S3::S3Error MakeThrottleError() {
    Aws::S3::S3Error error(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, "TooManyRequests", "Reduce request rate.", false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS);
    return error;
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);
    return columns;
}

NKikimrSchemeOp::TBackupTask MakeBackupTask(const TString& bucketName) {
    NKikimrSchemeOp::TBackupTask task;
    auto& settings = *task.MutableS3Settings();
    settings.SetBucket(bucketName);
    settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    settings.MutableExportDataSettings()->MutableYdbDump();
    auto& table = *task.MutableTable()->MutableColumnTableDescription();
    table.SetColumnShardCount(1);
    for (const TString& name : { TString("key"), TString("value") }) {
        auto& column = *table.MutableSchema()->AddColumns();
        column.SetName(name);
        column.SetType("String");
    }
    task.MutableTable()->MutableSelf();
    return task;
}

}   // namespace

Y_UNIT_TEST_SUITE(IScanS3Retry) {
    void TestUploaderMultipartExport(bool injectFailure, bool allowRetry, bool acknowledgedBatch = false) {
        const TString bucketName = TStringBuilder() << "iscan-retry-" << injectFailure << allowRetry << acknowledgedBatch;
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);

        TTestActorRuntime runtime;
        runtime.SetDispatchTimeout(TDuration::Seconds(10));
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        runtime.Initialize({ new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {} });

        // Each large row fills an S3 part. The second row fills the next buffer while
        // the first part is in flight, putting TUploaderActor into Sleep.
        arrow::StringBuilder keys;
        arrow::StringBuilder values;
        const TString value(6 << 20, 'x');
        TString expectedCsv;
        for (ui32 i = 0; i < 4; ++i) {
            const TString key = ToString(i);
            UNIT_ASSERT(keys.Append(key.data(), key.size()).ok());
            UNIT_ASSERT(values.Append(value.data(), value.size()).ok());
            expectedCsv += TStringBuilder() << '"' << key << "\",\"" << value << "\"\n";
        }
        std::shared_ptr<arrow::Array> keyArray;
        std::shared_ptr<arrow::Array> valueArray;
        UNIT_ASSERT(keys.Finish(&keyArray).ok());
        UNIT_ASSERT(values.Finish(&valueArray).ok());
        const auto schema = arrow::schema({ arrow::field("key", arrow::binary()), arrow::field("value", arrow::binary()) });
        const auto batch = arrow::RecordBatch::Make(schema, 4, { keyArray, valueArray });

        auto task = MakeBackupTask(bucketName);
        task.SetNumberOfRetries(allowRetry ? 1 : 0);
        task.SetEnableChecksums(false);
        task.MutableScanSettings()->SetRowsBatchSize(1);

        ui32 injectedErrors = 0;
        ui32 resetCount = 0;
        ui32 retryWakeups = 0;
        TActorId s3Uploader;
        auto uploadObserver = runtime.AddObserver<NWrappers::TEvExternalStorage::TEvUploadPartResponse>([&](auto& ev) {
            if (injectFailure && !injectedErrors) {
                UNIT_ASSERT_C(ev->Get()->Result.IsSuccess(), ev->Get()->ToString());
                ++injectedErrors;
                s3Uploader = ev->Recipient;
                using TResult = std::decay_t<decltype(ev->Get()->Result)>;
                ev->Get()->Result = TResult(MakeThrottleError());
            }
        });
        auto resetObserver = runtime.AddObserver<NDataShard::TEvExportScan::TEvReset>([&](auto&) {
            ++resetCount;
        });

        // Deliver the real retry timer immediately instead of waiting for the
        // uploader's one-minute backoff. Storage responses drive the retry.
        auto previousScheduledFilter = runtime.SetScheduledEventFilter(&TTestActorRuntime::NopFilterFunc);
        runtime.SetScheduledEventFilter(
            [&, previousScheduledFilter](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant& deadline) {
                if (s3Uploader && ev->GetRecipientRewrite() == s3Uploader && ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    ++retryWakeups;
                    r.Send(ev.Release(), 0, true);
                    return true;
                }
                return previousScheduledFilter(r, ev, delay, deadline);
            });

        const auto edge = runtime.AllocateEdgeActor();
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NColumnShard::NBackup::CreateExportUploaderActor(edge, task, exportFactory.get(), MakeYdbColumns(), 0);
        const auto exporter = runtime.Register(actor.release());
        if (acknowledgedBatch) {
            UNIT_ASSERT(keys.Append("ack").ok());
            UNIT_ASSERT(values.Append("saved").ok());
            UNIT_ASSERT(keys.Finish(&keyArray).ok());
            UNIT_ASSERT(values.Finish(&valueArray).ok());
            auto prefix = arrow::RecordBatch::Make(schema, 1, { keyArray, valueArray });
            runtime.Send(new IEventHandle(exporter, edge, new TEvPrivate::TEvBackupExportRecordBatch(prefix, false)), 0, true);
            TAutoPtr<IEventHandle> intermediateHandle;
            const auto* intermediate = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupExportRecordBatchResult>(intermediateHandle);
            UNIT_ASSERT(!intermediate->IsFinish);
            UNIT_ASSERT_VALUES_EQUAL(injectedErrors, 0);
            expectedCsv = "\"ack\",\"saved\"\n" + expectedCsv;
        }
        runtime.Send(new IEventHandle(exporter, edge, new TEvPrivate::TEvBackupExportRecordBatch(batch, true)), 0, true);

        TAutoPtr<IEventHandle> handle;
        if (injectFailure) {
            const auto* error = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupExportError>(handle);
            UNIT_ASSERT(error);
            if (allowRetry) {
                UNIT_ASSERT_STRING_CONTAINS(error->ErrorMessage, "scan reset (EScan::Reset)");
                UNIT_ASSERT_STRING_CONTAINS(error->ErrorMessage, "restarting the scan is not supported");
                UNIT_ASSERT_STRING_CONTAINS(error->ErrorMessage, "HTTP 429 TooManyRequests");
                UNIT_ASSERT_STRING_CONTAINS(error->ErrorMessage, "export s3");
            } else {
                UNIT_ASSERT_STRING_CONTAINS(error->ErrorMessage, "Reduce request rate.");
                UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount(bucketName, s3Client), 0);
            }
            UNIT_ASSERT_VALUES_EQUAL(injectedErrors, 1);
            UNIT_ASSERT_VALUES_EQUAL(retryWakeups, allowRetry ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(resetCount, allowRetry ? 1 : 0);
            return;
        }
        const auto* result = runtime.GrabEdgeEvent<TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(result->IsFinish);
        UNIT_ASSERT_VALUES_EQUAL(resetCount, 0);

        const TString dataKey = NDataShard::NBackupRestoreTraits::DataKeySuffix(0, EDataFormat::YdbDump, ECompressionCodec::None, false);
        Aws::S3::Model::GetObjectRequest request;
        auto response = s3Client.GetObject(request.WithBucket(bucketName).WithKey(dataKey));
        UNIT_ASSERT_C(response.IsSuccess(), response.GetError().GetMessage());
        const std::string actualCsv((std::istreambuf_iterator<char>(response.GetResult().GetBody())), std::istreambuf_iterator<char>());
        UNIT_ASSERT_VALUES_EQUAL(actualCsv.size(), expectedCsv.size());
        UNIT_ASSERT_C(TStringBuf(actualCsv.data(), actualCsv.size()) == expectedCsv, "Exported CSV differs from the input rows");
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount(bucketName, s3Client), 0);
    }

    Y_UNIT_TEST(UploaderMultipartExport) {
        TestUploaderMultipartExport(false, true);
    }

    Y_UNIT_TEST(UploaderReportsResetAfterS3Throttling) {
        TestUploaderMultipartExport(true, true);
    }

    Y_UNIT_TEST(UploaderReportsResetAfterAcknowledgedBatch) {
        TestUploaderMultipartExport(true, true, true);
    }

    Y_UNIT_TEST(UploaderPreservesS3ErrorWithoutRetry) {
        TestUploaderMultipartExport(true, false);
    }
}

}   // namespace NKikimr
