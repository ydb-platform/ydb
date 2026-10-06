#include <ydb/library/yql/providers/s3/actors/yql_arrow_push_down.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/statistics.h>

#include <google/protobuf/text_format.h>

#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

#ifdef ENABLE_S3_READ_ACTOR_TESTS
#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <util/generic/algorithm.h>
#endif

namespace NYql::NPathGenerator {

struct TFileMetaDataBuilder {
    struct TRowGroupBuilder {
        TRowGroupBuilder(TFileMetaDataBuilder* parent,
                         std::shared_ptr<parquet::SchemaDescriptor> schema,
                         parquet::RowGroupMetaDataBuilder* rowGroup)
            : Parent(parent)
            , Schema(schema)
            , RowGroup(rowGroup)
        {}

        TRowGroupBuilder& AddColumnTimestampStatistics(int64_t columnId, const int64_t min, const int64_t max) {
            auto columnChunk = RowGroup->NextColumnChunk();
            auto stat = parquet::MakeStatistics<parquet::Int64Type>(Schema->Column(columnId));
            stat->SetMinMax(min, max);
            columnChunk->SetStatistics(stat->Encode());
            return *this;
        }

        TRowGroupBuilder& AddColumnFlbaStatistics(int64_t columnId, TString min, TString max) {
            auto columnChunk = RowGroup->NextColumnChunk();
            auto stat = parquet::MakeStatistics<parquet::FLBAType>(Schema->Column(columnId));
            parquet::FixedLenByteArray minFlba(reinterpret_cast<const uint8_t*>(min.data()));
            parquet::FixedLenByteArray maxFlba(reinterpret_cast<const uint8_t*>(max.data()));
            stat->SetMinMax(minFlba, maxFlba);
            columnChunk->SetStatistics(stat->Encode());
            return *this;
        }

        TRowGroupBuilder& AddColumnNullStatistics(int64_t nullCount) {
            auto columnChunk = RowGroup->NextColumnChunk();
            parquet::EncodedStatistics statistics;
            statistics.set_null_count(nullCount);
            columnChunk->SetStatistics(statistics);
            return *this;
        }

        TFileMetaDataBuilder& Build() {
            return *Parent;
        }

    private:
        TFileMetaDataBuilder* Parent;
        std::shared_ptr<parquet::SchemaDescriptor> Schema;
        parquet::RowGroupMetaDataBuilder* RowGroup;
    };

    TFileMetaDataBuilder(const TVector<std::shared_ptr<arrow::Field>>& columns) {
        auto schema = arrow::schema(columns);
        parquet::WriterProperties::Builder builder;
        auto properties = builder.build();
        
        UNIT_ASSERT(parquet::arrow::ToParquetSchema(schema.get(), *properties, &Schema) == ::arrow::Status::OK());

       FileMetadata = parquet::FileMetaDataBuilder::Make(Schema.get(), properties);
    }

    explicit TFileMetaDataBuilder(std::shared_ptr<parquet::SchemaDescriptor> schema)
        : Schema(std::move(schema))
    {
        parquet::WriterProperties::Builder builder;
        auto properties = builder.build();
        FileMetadata = parquet::FileMetaDataBuilder::Make(Schema.get(), properties);
    }

    TRowGroupBuilder AddRowGroup() {
        return TRowGroupBuilder(this, Schema, FileMetadata->AppendRowGroup());
    }

    std::unique_ptr<parquet::FileMetaData> Build() {
        return FileMetadata->Finish();
    }

private:
    std::unique_ptr<parquet::FileMetaDataBuilder> FileMetadata;
    std::shared_ptr<parquet::SchemaDescriptor> Schema;
};

std::shared_ptr<parquet::SchemaDescriptor> MakeLogicalUuidSchema(const TString& name) {
    auto node = parquet::schema::PrimitiveNode::Make(
        name,
        parquet::Repetition::REQUIRED,
        parquet::LogicalType::UUID(),
        parquet::Type::FIXED_LEN_BYTE_ARRAY,
        16);
    auto group = parquet::schema::GroupNode::Make(
        "schema", parquet::Repetition::REQUIRED, {node});
    auto schema = std::make_shared<parquet::SchemaDescriptor>();
    schema->Init(group);
    return schema;
}

NYql::NConnector::NApi::TPredicate BuildPredicate(const TString& text) {
    NYql::NConnector::NApi::TPredicate predicate;
    UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(text, &predicate));
    return predicate;
}

TString UuidValueField(const TString& bytes) {
    const ui64 low = LittleToHost(ReadUnaligned<ui64>(bytes.data()));
    const ui64 high = LittleToHost(ReadUnaligned<ui64>(bytes.data() + sizeof(ui64)));
    return TStringBuilder() << "low_128: " << low << " high_128: " << high;
}

Y_UNIT_TEST_SUITE(TArrowPushDown) {
    Y_UNIT_TEST(SimplePushDown) {
        TFileMetaDataBuilder builder{{
            arrow::field("field1", arrow::timestamp(arrow::TimeUnit::type::MILLI)),
            arrow::field("field2", arrow::int64()),
            arrow::field("field3", arrow::float64())
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnTimestampStatistics(0, TInstant::ParseIso8601("2024-03-01T00:00:00Z").MilliSeconds(), TInstant::ParseIso8601("2024-04-01T00:00:00Z").MilliSeconds())
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
                        R"proto(
                    comparison {
                        operation: L
                        left_value {
                            column: "field1"
                        }
                        right_value {
                            typed_value {
                                type {
                                    type_id: TIMESTAMP
                                }
                                value {
                                    int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                }
                            }
                        }
                    }
                )proto");

        auto rowGroups = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups[0], 0);
    }

    Y_UNIT_TEST(FilterEverything) {
        TFileMetaDataBuilder builder{{
            arrow::field("field1", arrow::timestamp(arrow::TimeUnit::type::MILLI)),
            arrow::field("field2", arrow::int64()),
            arrow::field("field3", arrow::float64())
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnTimestampStatistics(0, TInstant::ParseIso8601("2024-04-01T00:00:00Z").MilliSeconds(), TInstant::ParseIso8601("2024-04-13T00:00:00Z").MilliSeconds())
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
                        R"proto(
                    comparison {
                        operation: L
                        left_value {
                            column: "field1"
                        }
                        right_value {
                            typed_value {
                                type {
                                    type_id: TIMESTAMP
                                }
                                value {
                                    int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                }
                            }
                        }
                    }
                )proto");

        auto rowGroups = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups.size(), 0);
    }

    Y_UNIT_TEST(MatchSeveralRowGroups) {
        TFileMetaDataBuilder builder{{
            arrow::field("field1", arrow::timestamp(arrow::TimeUnit::type::MILLI)),
            arrow::field("field2", arrow::int64()),
            arrow::field("field3", arrow::float64())
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnTimestampStatistics(0, TInstant::ParseIso8601("2024-03-01T00:00:00Z").MilliSeconds(), TInstant::ParseIso8601("2024-04-01T00:00:00Z").MilliSeconds())
                                   .Build()
                                   .AddRowGroup()
                                   .AddColumnTimestampStatistics(0, TInstant::ParseIso8601("2024-02-01T00:00:00Z").MilliSeconds(), TInstant::ParseIso8601("2024-04-01T00:00:00Z").MilliSeconds())
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
                        R"proto(
                    comparison {
                        operation: L
                        left_value {
                            column: "field1"
                        }
                        right_value {
                            typed_value {
                                type {
                                    type_id: TIMESTAMP
                                }
                                value {
                                    int64_value: 1709290801000000 # 2024-03-01T11:00:01.000Z
                                }
                            }
                        }
                    }
                )proto");

        auto rowGroups = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups[1], 1);
    }

    Y_UNIT_TEST(UuidWithoutMinMaxKeepsGroup) {
        auto plainSchema = std::make_shared<parquet::SchemaDescriptor>();
        plainSchema->Init(parquet::schema::GroupNode::Make(
            "schema", parquet::Repetition::REQUIRED,
            {parquet::schema::PrimitiveNode::Make("id", parquet::Repetition::OPTIONAL,
                parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 16)}));
        const auto predicate = BuildPredicate(
            R"proto(comparison {
                operation: EQ
                left_value { column: "id" }
                right_value { typed_value { type { type_id: UUID } value { low_128: 0 high_128: 0 } } }
            })proto");
        for (const auto& schema : {MakeLogicalUuidSchema("id"), plainSchema}) {
            TFileMetaDataBuilder builder{schema};
            auto metadata = builder.AddRowGroup().AddColumnNullStatistics(2).Build().Build();
            const auto column = metadata->RowGroup(0)->ColumnChunk(0);
            UNIT_ASSERT(column->is_stats_set());
            UNIT_ASSERT(!column->statistics()->HasMinMax());
            const auto kept = NDq::MatchedRowGroups(metadata, predicate);
            UNIT_ASSERT_VALUES_EQUAL(kept.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(kept[0], 0);
        }
    }

    Y_UNIT_TEST(UuidLogicalTypePushDown) {
        const TString lo(16, '\x10');
        const TString hi(16, '\x20');
        const TString inside(16, '\x15');
        const TString outside(16, '\x30');

        TFileMetaDataBuilder builder{MakeLogicalUuidSchema("id")};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, lo, hi)
                                   .Build()
                            .Build();

        auto skipPredicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "id" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(outside) << R"proto( } } }
                }
            )proto");
        UNIT_ASSERT_VALUES_EQUAL(NDq::MatchedRowGroups(fileMetadata, skipPredicate).size(), 0);

        auto keepPredicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "id" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(inside) << R"proto( } } }
                }
            )proto");
        auto kept = NDq::MatchedRowGroups(fileMetadata, keepPredicate);
        UNIT_ASSERT_VALUES_EQUAL(kept.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(kept[0], 0);
    }

    Y_UNIT_TEST(UuidMatchSecondGroup) {
        const TString firstLo(16, '\x10');
        const TString firstHi(16, '\x11');
        const TString secondLo(16, '\x20');
        const TString secondHi(16, '\x21');
        const TString fromSecond(16, '\x20');

        TFileMetaDataBuilder builder{MakeLogicalUuidSchema("id")};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, firstLo, firstHi)
                                   .Build()
                                   .AddRowGroup()
                                   .AddColumnFlbaStatistics(0, secondLo, secondHi)
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "id" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(fromSecond) << R"proto( } } }
                }
            )proto");
        auto rowGroups = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rowGroups[0], 1);
    }

    Y_UNIT_TEST(FixedSizeBinaryWithoutUuidLogicalType) {
        const TString lo(16, '\x10');
        const TString hi(16, '\x20');
        const TString outside(16, '\x30');

        TFileMetaDataBuilder builder{{
            arrow::field("id", arrow::fixed_size_binary(16))
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, lo, hi)
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "id" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(outside) << R"proto( } } }
                }
            )proto");
        // pyarrow 5 writes Uuid as FLBA(16) without UUID logical type; skip using raw bytes.
        UNIT_ASSERT_VALUES_EQUAL(NDq::MatchedRowGroups(fileMetadata, predicate).size(), 0);
    }

    Y_UNIT_TEST(Flba16WithoutUuidLogicalTypeIsTreatedAsUuid) {
        // FLBA(16) with NONE logical type is treated as UUID (pyarrow 5 compatibility).
        const TString lo(16, '\x10');
        const TString hi(16, '\x20');
        const TString outside(16, '\x30');

        TFileMetaDataBuilder builder{{
            arrow::field("data", arrow::fixed_size_binary(16))
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, lo, hi)
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "data" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(outside) << R"proto( } } }
                }
            )proto");
        UNIT_ASSERT_VALUES_EQUAL(NDq::MatchedRowGroups(fileMetadata, predicate).size(), 0);
    }

    Y_UNIT_TEST(Flba16WithoutUuidLogicalTypeKeepGroup) {
        const TString lo(16, '\x10');
        const TString hi(16, '\x20');
        const TString inside(16, '\x15');

        TFileMetaDataBuilder builder{{
            arrow::field("data", arrow::fixed_size_binary(16))
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, lo, hi)
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "data" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(inside) << R"proto( } } }
                }
            )proto");
        auto kept = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(kept.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(kept[0], 0);
    }

    Y_UNIT_TEST(Flba32WithoutUuidLogicalTypeNotTreatedAsUuid) {
        // FLBA(32) with NONE logical type should NOT be treated as UUID.
        const TString lo(32, '\x10');
        const TString hi(32, '\x20');
        const TString outside(16, '\x30');

        TFileMetaDataBuilder builder{{
            arrow::field("data", arrow::fixed_size_binary(32))
        }};
        auto fileMetadata = builder.AddRowGroup()
                                   .AddColumnFlbaStatistics(0, lo, hi)
                                   .Build()
                            .Build();

        auto predicate = BuildPredicate(
            TStringBuilder() << R"proto(
                comparison {
                    operation: EQ
                    left_value { column: "data" }
                    right_value { typed_value { type { type_id: UUID } value { )proto"
                             << UuidValueField(outside) << R"proto( } } }
                }
            )proto");
        auto kept = NDq::MatchedRowGroups(fileMetadata, predicate);
        UNIT_ASSERT_VALUES_EQUAL(kept.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(kept[0], 0);
    }
}

}

#ifdef ENABLE_S3_READ_ACTOR_TESTS

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

#endif
