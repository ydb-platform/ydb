#include <ydb/core/tx/datashard/read_table_scan.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <variant>
#include <vector>

namespace NKikimr::NGRpcService {
namespace {

using namespace NYdb;

constexpr size_t WarmupSerializations = 100;
constexpr size_t MeasuredSerializations = 1000;

struct TMockInput {
    TVector<NScheme::TTypeInfo> Types;
    TVector<bool> NotNull;
    TVector<TString> Names;
    TOwnedCellVec Row;
};

TMockInput MakeMockInput() {
    const TString stringValue(128, 's');
    const TString utf8Value(128, 'u');
    const auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(
        R"({"tag_01":"value_01","tag_02":"value_02","tag_03":"value_03","tag_04":"value_04"})");
    UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
    const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);

    TVector<TCell> cells{
        TCell::Make<i32>(1),
        TCell::Make<i32>(2),
        TCell::Make<i32>(3),
        TCell(stringValue.data(), stringValue.size()),
        TCell::Make<bool>(true),
        TCell::Make<ui64>(1'700'000'000'000'000ull),
        TCell::Make<bool>(false),
        TCell::Make<ui64>(1'800'000'000'000'000ull),
        TCell::Make<i8>(4),
        TCell(binaryJson.Data(), binaryJson.Size()),
        TCell(utf8Value.data(), utf8Value.size()),
    };

    return {
        .Types = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Int32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int32),
            NScheme::TTypeInfo(NScheme::NTypeIds::String),
            NScheme::TTypeInfo(NScheme::NTypeIds::Bool),
            NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp),
            NScheme::TTypeInfo(NScheme::NTypeIds::Bool),
            NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int8),
            NScheme::TTypeInfo(NScheme::NTypeIds::JsonDocument),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        },
        .NotNull = {
            true,
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
        },
        .Names = {
            "field_01",
            "field_02",
            "field_03",
            "field_04",
            "field_05",
            "field_06",
            "field_07",
            "field_08",
            "field_09",
            "field_10",
            "field_11",
        },
        .Row = TOwnedCellVec(cells),
    };
}

Ydb::ResultSet SerializeMockRowWithValueBuilder(const TMockInput& input) {
    TValueBuilder valueBuilder;
    valueBuilder.BeginStruct();

    const auto& cells = input.Row;
    for (size_t i = 0; i < input.Types.size(); ++i) {
        valueBuilder.AddMember(input.Names[i]);
        if (cells[i].IsNull()) {
            valueBuilder.EmptyOptional(static_cast<EPrimitiveType>(input.Types[i].GetTypeId()));
        } else if (input.NotNull[i]) {
            ProtoValueFromCell(valueBuilder, input.Types[i], cells[i]);
        } else {
            valueBuilder.BeginOptional();
            ProtoValueFromCell(valueBuilder, input.Types[i], cells[i]);
            valueBuilder.EndOptional();
        }
    }
    valueBuilder.EndStruct();

    auto protoRow = TProtoAccessor::GetProto(valueBuilder.Build());
    Ydb::ResultSet resultSet;
    *resultSet.add_rows() = std::move(protoRow);
    return resultSet;
}

Ydb::ResultSet SerializeMockRowDirectly(const TMockInput& input) {
    Ydb::ResultSet resultSet;
    resultSet.mutable_rows()->Reserve(1);
    TString error;
    UNIT_ASSERT_C(NDataShard::AddRowToYdbResultSet(resultSet, input.Types, input.Row, error), error);
    return resultSet;
}

double NearestRankP99Ms(std::vector<double>& samples) {
    UNIT_ASSERT_VALUES_EQUAL(samples.size(), MeasuredSerializations);
    std::sort(samples.begin(), samples.end());
    const size_t rank = static_cast<size_t>(std::ceil(0.99 * samples.size()));
    return samples[rank - 1];
}

} // namespace

Y_UNIT_TEST_SUITE(ReadRowsBenchmark) {
    Y_UNIT_TEST(P99SingleRow) {
        const auto input = MakeMockInput();
        UNIT_ASSERT_VALUES_EQUAL(input.Types.size(), input.NotNull.size());
        UNIT_ASSERT_VALUES_EQUAL(input.Types.size(), input.Names.size());
        UNIT_ASSERT_VALUES_EQUAL(input.Types.size(), input.Row.size());

        const auto valueBuilderResult = SerializeMockRowWithValueBuilder(input);
        const auto directResult = SerializeMockRowDirectly(input);
        UNIT_ASSERT_VALUES_EQUAL(valueBuilderResult.rows_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(directResult.rows_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(directResult.rows(0).items_size(), static_cast<int>(input.Types.size()));
        UNIT_ASSERT_VALUES_EQUAL(valueBuilderResult.SerializeAsString(), directResult.SerializeAsString());

        ui64 warmupBytes = 0;
        for (size_t i = 0; i < WarmupSerializations; ++i) {
            warmupBytes += SerializeMockRowWithValueBuilder(input).ByteSizeLong();
            warmupBytes += SerializeMockRowDirectly(input).ByteSizeLong();
        }

        std::vector<double> valueBuilderLatencyMs;
        valueBuilderLatencyMs.reserve(MeasuredSerializations);
        std::vector<double> directLatencyMs;
        directLatencyMs.reserve(MeasuredSerializations);
        ui64 valueBuilderBytes = 0;
        ui64 directBytes = 0;

        const auto measure = [&](auto serializer, std::vector<double>& latencyMs, ui64& bytes) {
            const auto startedAt = std::chrono::steady_clock::now();
            const auto result = serializer(input);
            const auto finishedAt = std::chrono::steady_clock::now();

            latencyMs.push_back(
                std::chrono::duration<double, std::milli>(finishedAt - startedAt).count());
            bytes += result.ByteSizeLong();
        };

        for (size_t i = 0; i < MeasuredSerializations; ++i) {
            if (i % 2 == 0) {
                measure(SerializeMockRowWithValueBuilder, valueBuilderLatencyMs, valueBuilderBytes);
                measure(SerializeMockRowDirectly, directLatencyMs, directBytes);
            } else {
                measure(SerializeMockRowDirectly, directLatencyMs, directBytes);
                measure(SerializeMockRowWithValueBuilder, valueBuilderLatencyMs, valueBuilderBytes);
            }
        }

        const double valueBuilderP99Ms = NearestRankP99Ms(valueBuilderLatencyMs);
        const double directP99Ms = NearestRankP99Ms(directLatencyMs);
        Cerr << "ReadRows serialization benchmark: serializations=" << MeasuredSerializations
             << " rows_per_serialization=1"
             << " columns=11"
             << " value_builder_protobuf_bytes=" << valueBuilderBytes / MeasuredSerializations
             << " direct_protobuf_bytes=" << directBytes / MeasuredSerializations
             << " warmup_checksum=" << warmupBytes
             << " value_builder_p99_ms=" << valueBuilderP99Ms
             << " direct_p99_ms=" << directP99Ms
             << " p99_speedup=" << valueBuilderP99Ms / directP99Ms << Endl;
    }
}

} // namespace NKikimr::NGRpcService
