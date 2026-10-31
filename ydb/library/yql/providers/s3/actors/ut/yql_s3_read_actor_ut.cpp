#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/events/events.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/schema.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/metadata.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/stream/str.h>

#include <algorithm>

namespace NYql::NDq {
namespace {

using namespace NActors;
using namespace NKikimr::NMiniKQL;

// Serve the file through the same ranged HTTP interface as the production reader.
class TParquetGateway: public IHTTPGateway {
public:
    TString Body;

    void Upload(TString, THeaders, TString, TOnResult, bool, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected upload");
    }

    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected delete");
    }

    void Download(
        TString, THeaders, size_t offset, size_t size, TOnResult callback, TString, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_ASSERT_C(offset <= Body.size() && size <= Body.size() - offset, "invalid HTTP range " << offset << "+" << size);
        callback(TResult(TContent(Body.substr(offset, size), 206)));
    }

    TCancelHook Download(TString, THeaders, size_t, size_t, TOnDownloadStart, TOnNewDataPart, TOnDownloadFinish,
        const ::NMonitoring::TDynamicCounters::TCounterPtr&, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected streaming download");
        return {};
    }

    ui64 GetBuffersSizePerStream() override {
        return 0;
    }

    void UpdatePoolCaps(THashMap<TWorkScope, size_t>) override {
    }

};

NS3::TSource ParquetSource(ui64 parallelRowGroups) {
    NS3::TSource source;
    source.SetUrl("http://fake/");
    source.SetFormat("parquet");
    source.SetRowType(R"(["StructType";[["ts";["DataType";"Timestamp"]]]])");
    source.SetParallelRowGroupCount(parallelRowGroups);
    return source;
}

std::shared_ptr<arrow::Schema> ParquetSchema() {
    return arrow::schema({ arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO), false) });
}

std::shared_ptr<arrow::Array> MakeTimestamps(int begin, int end) {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    for (int i = begin; i < end; ++i) {
        UNIT_ASSERT(builder.Append(i * 1000000LL).ok());
    }
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    return array;
}

std::vector<i64> ExpectedTimestamps(int rows) {
    std::vector<i64> expected;
    for (i64 i = 0; i < rows; ++i) {
        expected.push_back(i * 1000000);
    }
    return expected;
}

// Valid control: one timestamp column, rowGroupSize rows per row group.
TString MakeParquet(int rows, int rowGroupSize = 1) {
    auto table = arrow::Table::Make(ParquetSchema(), { MakeTimestamps(0, rows) });
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, rowGroupSize).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString AppendFooter(TString body, parquet::FileMetaDataBuilder& builder) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    builder.Finish()->WriteTo(sink.get());
    auto footer = sink->Finish().ValueOrDie();
    body.append(reinterpret_cast<const char*>(footer->data()), footer->size());
    // The Parquet trailer stores the footer length in little-endian order.
    for (ui32 i = 0; i < 4; ++i) {
        body.push_back(static_cast<char>((footer->size() >> (8 * i)) & 0xff));
    }
    return body + "PAR1";
}

// Real data row groups behind a leading row group whose column chunk is zero bytes long.
TString MakeParquetWithLeadingEmptyRowGroup(int rows, int rowGroupSize) {
    const TString source = MakeParquet(rows, rowGroupSize);
    auto input = std::make_shared<arrow::io::BufferReader>(reinterpret_cast<const ui8*>(source.data()), source.size());
    auto metadata = parquet::ParquetFileReader::Open(input)->metadata();
    ui32 footerSize = 0;
    for (ui32 i = 0; i < 4; ++i) {
        footerSize |= static_cast<ui32>(static_cast<ui8>(source[source.size() - 8 + i])) << (8 * i);
    }
    auto properties = parquet::WriterProperties::Builder().build();
    auto builder = parquet::FileMetaDataBuilder::Make(metadata->schema(), properties);
    auto emptyRowGroup = builder->AppendRowGroup();
    emptyRowGroup->NextColumnChunk()->Finish(0, 0, -1, 4, 0, 0, false, false, {}, {});
    emptyRowGroup->set_num_rows(0);
    emptyRowGroup->Finish(0);
    for (int i = 0; i < metadata->num_row_groups(); ++i) {
        const auto rowGroupMetadata = metadata->RowGroup(i);
        const auto column = rowGroupMetadata->ColumnChunk(0);
        auto rowGroup = builder->AppendRowGroup();
        rowGroup->NextColumnChunk()->Finish(column->num_values(), column->has_dictionary_page() ? column->dictionary_page_offset() : 0, -1,
            column->data_page_offset(), column->total_compressed_size(), column->total_uncompressed_size(), column->has_dictionary_page(), false,
            {}, {});
        rowGroup->set_num_rows(rowGroupMetadata->num_rows());
        rowGroup->Finish(rowGroupMetadata->total_byte_size());
    }
    return AppendFooter(source.substr(0, source.size() - 8 - footerSize), *builder);
}

struct TReadResult {
    bool Finished = false;
    ui64 Rows = 0;
    ui64 Batches = 0;
    std::vector<i64> Values;
    std::vector<NDqProto::StatusIds::StatusCode> Errors;
    TString Issues;
};

// All callbacks and observations run on the simulated actor runtime's single thread.
class TParquetReader {
public:
    TReadResult Result;

    TParquetReader() {
        Runtime.AddLocalService(FakeActorId, TActorSetupCmd(new TFakeActor(std::make_shared<TAsyncInputPromises>(),
                                                                std::make_shared<TAsyncOutputPromises>()), TMailboxType::Simple, 0));
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(
            NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == IDqComputeActorAsyncInput::TEvAsyncInputError::EventType) {
                auto* error = ev->Get<IDqComputeActorAsyncInput::TEvAsyncInputError>();
                Result.Errors.push_back(error->FatalCode);
                Result.Issues += error->Issues.ToOneLineString();
            } else if (ev->GetTypeRewrite() == TEvS3Provider::TEvNextRecordBatch::EventType) {
                const auto& batch = ev->Get<TEvS3Provider::TEvNextRecordBatch>()->Batch;
                Result.Rows += batch->num_rows();
                ++Result.Batches;
                if (batch->num_columns()) {
                    auto values = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
                    for (i64 i = 0; i < values->length(); ++i) {
                        Result.Values.push_back(values->Value(i));
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
    }

    ~TParquetReader() {
        Execute([](TFakeActor& actor) {
            actor.Terminate();
        });
    }

    void Start(std::shared_ptr<TParquetGateway> gateway, NS3::TSource source, ui64 dataInflight = 200_MB, bool batched = true,
        ui64 batchDecodeTargetBytes = 16_MB)
    {
        NS3::TRange range;
        auto* path = range.AddPaths();
        path->SetName("file.parquet");
        path->SetSize(gateway->Body.size());
        path->SetRead(true);
        TStringStream encodedRange;
        range.Save(&encodedRange);
        Execute([&](TFakeActor& actor) {
            TS3ReadActorFactoryConfig config;
            config.DataInflight = dataInflight;
            config.BatchedRowGroupDecode = batched;
            config.BatchDecodeTargetBytes = batchDecodeTargetBytes;
            auto [input, inputActor] =
                CreateS3ReadActor(actor.TypeEnv, actor.HolderFactory, std::shared_ptr<TScopedAlloc>(&actor.Alloc, [](TScopedAlloc*) {
                }), gateway, std::move(source), 0, TCollectStatsLevel::None,
                    TTxId{}, {}, {}, { encodedRange.Str() }, FakeActorId, CreateStructuredTokenCredentialsFactory(), GetHTTPDefaultRetryPolicy(),
                    config, nullptr, nullptr, std::make_shared<TGuaranteeQuotaManager>(dataInflight, dataInflight), false);
            actor.InitAsyncInput(input, inputActor);
        });
    }

    void Pump() {
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        Execute([&](TFakeActor& actor) {
            TMaybe<TInstant> watermark;
            TUnboxedValueBatch buffer;
            actor.DqAsyncInput->GetAsyncInputData(buffer, watermark, Result.Finished, 1_MB);
        });
    }

    void ReadToEnd() {
        for (ui32 step = 0; step < 100 && !Result.Finished && Result.Errors.empty(); ++step) {
            Pump();
        }
        UNIT_ASSERT_C(Result.Finished || !Result.Errors.empty(), "HTTP reader did not complete");
    }

private:
    void Execute(TCallback callback) {
        std::exception_ptr exception;
        auto promise = NThreading::NewPromise();
        Runtime.Send(new IEventHandle(FakeActorId, Runtime.AllocateEdgeActor(), new TEvPrivate::TEvExecute(promise, callback, exception)));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return promise.HasValue();
        };
        Runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(promise.HasValue(), "compute actor callback did not complete");
        if (exception) {
            std::rethrow_exception(exception);
        }
    }

    TTestActorRuntimeBase Runtime{ 1 };
    const TActorId FakeActorId{ 0, "FakeActor" };
};

TReadResult ReadParquet(TString body, ui64 parallelRowGroups, bool batched, ui64 batchDecodeTargetBytes = 16_MB) {
    TParquetReader reader;
    auto gateway = std::make_shared<TParquetGateway>();
    gateway->Body = std::move(body);
    reader.Start(gateway, ParquetSource(parallelRowGroups), 200_MB, batched, batchDecodeTargetBytes);
    reader.ReadToEnd();
    UNIT_ASSERT_C(reader.Result.Errors.empty(), reader.Result.Issues);
    UNIT_ASSERT(reader.Result.Finished);
    std::sort(reader.Result.Values.begin(), reader.Result.Values.end());
    return reader.Result;
}

}   // namespace

Y_UNIT_TEST_SUITE(TS3ParquetDecode) {
    Y_UNIT_TEST(LargeRowGroupBatched) {
        for (ui64 parallel : { 0, 1, 2 }) {
            const auto result = ReadParquet(MakeParquet(1000, 500), parallel, true, 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(result.Rows, 1000);
            UNIT_ASSERT_C(result.Batches > 2, "batches=" << result.Batches);
            UNIT_ASSERT_VALUES_EQUAL(result.Values, ExpectedTimestamps(1000));
        }
    }

    Y_UNIT_TEST(LargeRowGroupWhole) {
        for (ui64 parallel : { 0, 1, 2 }) {
            const auto result = ReadParquet(MakeParquet(1000, 500), parallel, false, 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(result.Rows, 1000);
            UNIT_ASSERT_VALUES_EQUAL(result.Batches, 2);
            UNIT_ASSERT_VALUES_EQUAL(result.Values, ExpectedTimestamps(1000));
        }
    }

    Y_UNIT_TEST(LeadingEmptyRowGroup) {
        for (bool batched : { true, false }) {
            for (ui64 parallel : { 0, 1, 2 }) {
                const auto result = ReadParquet(MakeParquetWithLeadingEmptyRowGroup(12, 3), parallel, batched);
                UNIT_ASSERT_VALUES_EQUAL_C(result.Rows, 12, "batched=" << batched << " parallel=" << parallel);
                UNIT_ASSERT_VALUES_EQUAL(result.Values, ExpectedTimestamps(12));
            }
        }
    }
}

}   // namespace NYql::NDq
