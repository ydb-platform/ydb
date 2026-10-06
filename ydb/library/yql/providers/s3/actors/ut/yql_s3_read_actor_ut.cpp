#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <util/generic/algorithm.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NYql::NDq {
namespace {

using namespace NKikimr::NMiniKQL;

class TRangeGateway final : public IHTTPGateway {
public:
    explicit TRangeGateway(TString data)
        : Data(std::move(data))
    {}

    void Download(TString, THeaders, size_t offset, size_t size, TOnResult callback,
                  TString, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        callback(TResult(TContent(Data.substr(offset, size), 206)));
    }

    void Upload(TString, THeaders, TString, TOnResult, bool, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected upload");
    }
    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected delete");
    }
    TCancelHook Download(TString, THeaders, size_t, size_t, TOnDownloadStart, TOnNewDataPart,
                         TOnDownloadFinish, const NMonitoring::TDynamicCounters::TCounterPtr&,
                         IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected streaming download");
        return {};
    }
    ui64 GetBuffersSizePerStream() override { return 0; }
    void UpdatePoolCaps(THashMap<TWorkScope, size_t>) override {}

private:
    const TString Data;
};

struct TQuotaManager final : IMemoryQuotaManager {
    bool AllocateQuota(ui64 size, bool) override { Quota += size; return true; }
    void FreeQuota(ui64 size) override { Quota -= size; }
    ui64 GetCurrentQuota() const override { return Quota; }
    ui64 GetMaxMemorySize() const override { return 1ULL << 30; }
    i64 GetMemoryAvailability() const override { return GetMaxMemorySize() - Quota; }
    TString MemoryConsumptionDetails() const override { return {}; }
    ui64 Quota = 0;
};

TString MakeParquet(bool nullLastGroup) {
    arrow::FixedSizeBinaryBuilder ids(arrow::fixed_size_binary(16));
    arrow::UInt64Builder values;
    for (ui64 group = 0; group < (nullLastGroup ? 7u : 8u); ++group) {
        const TString id(16, static_cast<char>(group + 1));
        for (ui64 row = 0; row < 4; ++row) {
            UNIT_ASSERT((nullLastGroup && group == 6 ? ids.AppendNull() : ids.Append(id)).ok());
            UNIT_ASSERT(values.Append(group * 4 + row).ok());
        }
    }
    std::shared_ptr<arrow::Array> idArray, valueArray;
    UNIT_ASSERT(ids.Finish(&idArray).ok());
    UNIT_ASSERT(values.Finish(&valueArray).ok());
    auto table = arrow::Table::Make(arrow::schema({
        arrow::field("id", arrow::fixed_size_binary(16)), arrow::field("value", arrow::uint64())}),
        {idArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 4).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

void CheckRead(ui64 parallelReaders, bool reorder, bool withPredicate, bool nullLastGroup = false) {
    const auto data = MakeParquet(nullLastGroup);
    TFakeCASetup setup;
    std::unique_ptr<THolderFactory> holder;
    auto error = setup.AsyncInputPromises->FatalError.GetFuture();
    setup.Execute([&](TFakeActor& actor) {
        holder = std::make_unique<THolderFactory>(actor.Alloc.Ref(), actor.MemoryInfo, actor.FunctionRegistry.Get());
        NS3::TSource source;
        source.SetUrl("http://unit-test/");
        source.SetFormat("parquet");
        source.SetRowType(R"(["StructType";[["value";["DataType";"Uint64"]]]])");
        if (nullLastGroup) {
            source.SetRowType(R"(["StructType";[["id";["OptionalType";["DataType";"Uuid"]]];["value";["DataType";"Uint64"]]]])");
        }
        source.SetParallelRowGroupCount(parallelReaders);
        source.SetRowGroupReordering(reorder);
        if (withPredicate) {
            // Skip an interior group, then read more groups than there are readers.
            auto* comparison = source.MutablePredicate()->mutable_comparison();
            comparison->set_operation(NConnector::NApi::TPredicate::TComparison::NE);
            comparison->mutable_left_value()->set_column("id");
            auto* constant = comparison->mutable_right_value()->mutable_typed_value();
            constant->mutable_type()->set_type_id(Ydb::Type::UUID);
            const TString id(16, '\x04');
            constant->mutable_value()->set_low_128(LittleToHost(ReadUnaligned<ui64>(id.data())));
            constant->mutable_value()->set_high_128(LittleToHost(ReadUnaligned<ui64>(id.data() + 8)));
        }
        NS3::TRange range;
        auto* path = range.AddPaths();
        path->SetName("data.parquet");
        path->SetSize(data.size());
        path->SetRead(true);
        TStringStream rangeData;
        range.Save(&rangeData);
        const auto [input, reader] = CreateS3ReadActor(actor.TypeEnv, *holder, nullptr,
            std::make_shared<TRangeGateway>(data), std::move(source), 0, TCollectStatsLevel::None,
            "test", {}, {}, {rangeData.Str()}, actor.SelfId(),
            CreateStructuredTokenCredentialsFactory(), IHTTPGateway::TRetryPolicy::GetNoRetryPolicy(),
            {}, nullptr, nullptr, std::make_shared<TQuotaManager>(), false);
        actor.InitAsyncInput(input, reader);
    });
    TVector<ui64> rows;
    bool finished = false;
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!finished && !error.HasValue() && TInstant::Now() < deadline) {
        NThreading::TFuture<void> ready;
        setup.Execute([&](TFakeActor& actor) {
            TUnboxedValueBatch batch;
            TMaybe<TInstant> watermark;
            actor.DqAsyncInput->GetAsyncInputData(batch, watermark, finished, 1 << 20);
            batch.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                const auto block = value.GetElement(nullLastGroup ? 2 : 1);
                auto array = TArrowBlock::From(block).GetDatum().make_array();
                const auto& numbers = static_cast<const arrow::UInt64Array&>(*array);
                for (int64_t i = 0; i < numbers.length(); ++i) {
                    rows.push_back(numbers.Value(i));
                }
            });
            ready = setup.AsyncInputPromises->NewAsyncInputDataArrived.GetFuture();
        });
        if (!finished) {
            ready.Wait(TDuration::MilliSeconds(10));
        }
    }
    setup.Terminate();
    setup.Execute([&](TFakeActor&) { holder.reset(); });
    UNIT_ASSERT_C(!error.HasValue(), error.HasValue() ? error.GetValue().ToString() : "");
    UNIT_ASSERT_C(finished, "S3 reader did not finish after skipping a Parquet row group");
    Sort(rows);
    TVector<ui64> expected;
    for (ui64 i = 0; i < (nullLastGroup ? 28u : 32u); ++i) {
        if (!withPredicate || i / 4 != 3) {
            expected.push_back(i);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(TS3ReadActorPushdown) {
    Y_UNIT_TEST(SkippedRowGroupsWithSingleReader) {
        CheckRead(1, false, true);
    }
    Y_UNIT_TEST(SkippedRowGroupsWithParallelReaders) {
        CheckRead(2, true, true);
    }
    Y_UNIT_TEST(SkippedRowGroupDoesNotHangWithPrefetchedGroups) {
        CheckRead(5, true, true, true);
    }
    Y_UNIT_TEST(ReadAllRowGroupsWithoutPredicate) {
        CheckRead(2, true, false);
    }
}
} // namespace NYql::NDq
