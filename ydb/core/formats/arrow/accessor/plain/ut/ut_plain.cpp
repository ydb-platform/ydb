#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(PlainArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;

    Y_UNIT_TEST(DecodeStatsUseActualPayloadCodecAndInvocation) {
        using namespace NKikimr;
        using namespace NKikimr::NArrow::NSerialization;
        arrow::UInt64Builder builder;
        UNIT_ASSERT(builder.AppendValues(std::vector<ui64>{1, 2, 3, 4}).ok());
        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        auto schema = arrow::schema({arrow::field("val", arrow::uint64())});
        auto batch = arrow::RecordBatch::Make(schema, 4, {array});
        for (const auto codec : {arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
            TNativeSerializer writer(codec);
            const TString encoded = writer.SerializePayload(batch);
            // Reader write settings deliberately differ from the actual payload.
            auto reader = std::make_shared<TNativeSerializer>(codec == arrow::Compression::UNCOMPRESSED
                                                                  ? arrow::Compression::ZSTD
                                                                  : arrow::Compression::UNCOMPRESSED);
            auto loader = std::make_shared<TColumnLoader>(TSerializerContainer(reader),
                                                          TConstructorContainer::GetDefaultConstructor(), arrow::field("price", arrow::uint64()), nullptr, 1);
            auto collector = std::make_shared<NScanStats::TCollector>(1);
            collector->BindScanId("decode");
            auto parent = collector->Start(NScanStats::TStep::Filter, "FILTER", "price > 0");
            {
                NScanStats::TScope scope(parent);
                TDeserializeChunkedArray lazy(4, loader, encoded, true);
                UNIT_ASSERT_VALUES_EQUAL(lazy.GetChunkedArray()->length(), 4);
                UNIT_ASSERT_VALUES_EQUAL(lazy.GetChunkedArray()->length(), 4);
                UNIT_ASSERT_VALUES_EQUAL(loader->ApplyVerified(encoded, 4)->GetRecordsCount(), 4);
                TDeserializeChunkedArray predefined(4, loader, std::make_shared<TTrivialArray>(array), true);
                UNIT_ASSERT_VALUES_EQUAL(predefined.GetChunkedArray()->length(), 4);
                UNIT_ASSERT(loader->ApplyConclusion("bad", 4).IsFail());
            }
            parent->Finish();
            const auto report = collector->Drain(true);
            UNIT_ASSERT(report->GetFinished());
            UNIT_ASSERT_VALUES_EQUAL(report->StepsSize(), 5);
            ui32 successful = 0;
            ui32 failed = 0;
            for (const auto& step : report->GetSteps()) {
                if (step.GetStepKind() != NScanStats::TStep::Decode) {
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(step.GetParentStepId(), parent->GetStepId());
                const auto& details = step.GetDetails().GetDecode();
                UNIT_ASSERT_VALUES_EQUAL(details.GetColumn(), "price");
                if (step.GetOutcome() == NScanStats::TStep::failed) {
                    ++failed;
                    UNIT_ASSERT(!details.HasDecodedRows());
                    UNIT_ASSERT(!details.HasDecodedOutputBytes());
                    UNIT_ASSERT(!details.HasCodec());
                } else {
                    ++successful;
                    UNIT_ASSERT_VALUES_EQUAL(details.GetEncodedInputBytes(), encoded.size());
                    UNIT_ASSERT_VALUES_EQUAL(details.GetDecodedRows(), 4);
                    UNIT_ASSERT_VALUES_EQUAL(details.GetDecodedOutputBytes(), 32);
                    const TString expected = codec == arrow::Compression::UNCOMPRESSED ? "uncompressed"
                                             : codec == arrow::Compression::LZ4_FRAME  ? "lz4"
                                                                                       : "zstd";
                    UNIT_ASSERT_VALUES_EQUAL(details.GetCodec(), expected);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(successful, 3);
            UNIT_ASSERT_VALUES_EQUAL(failed, 1);
        }
    }

    Y_UNIT_TEST(PlainAccessorsNoNulls) {
        auto value = std::make_shared<TTrivialArray>(std::make_shared<arrow::UInt8Scalar>(0));
        UNIT_ASSERT(value->HasNulls() == false);
        auto chunked = std::make_shared<TTrivialChunkedArray>(value->GetChunkedArray());
        UNIT_ASSERT(chunked->HasNulls() == false);
    }

    Y_UNIT_TEST(PlainAccessorsWithNulls) {
        auto value = std::make_shared<TTrivialArray>(arrow::MakeNullScalar(arrow::uint8()));
        UNIT_ASSERT(value->HasNulls() == true);
        auto chunked = std::make_shared<TTrivialChunkedArray>(value->GetChunkedArray());
        UNIT_ASSERT(chunked->HasNulls() == true);
    }
}; // Y_UNIT_TEST_SUITE(PlainArrayAccessor)
