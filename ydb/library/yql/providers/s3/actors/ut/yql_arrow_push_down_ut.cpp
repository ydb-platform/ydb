#include <ydb/library/yql/providers/s3/actors/yql_arrow_push_down.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/statistics.h>

#include <google/protobuf/text_format.h>

#include <cstring>

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
    ui64 low = 0;
    ui64 high = 0;
    memcpy(&low, bytes.data(), sizeof(ui64));
    memcpy(&high, bytes.data() + sizeof(ui64), sizeof(ui64));
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
