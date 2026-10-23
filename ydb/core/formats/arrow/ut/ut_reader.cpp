#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow {
namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeInfo = NScheme::TTypeInfo;

std::shared_ptr<arrow::RecordBatch> ExtractBatch(std::shared_ptr<arrow::Table> table) {
    std::shared_ptr<arrow::RecordBatch> batch;

    arrow::TableBatchReader reader(*table);
    auto result = reader.Next();
    Y_ABORT_UNLESS(result.ok());
    batch = *result;
    result = reader.Next();
    Y_ABORT_UNLESS(result.ok() && !(*result));
    return batch;
}

struct TDataRow {
    static const TTypeInfo* MakeTypeInfos() {
        static const TTypeInfo types[3] = {
            TTypeInfo(NTypeIds::Int32),
            TTypeInfo(NTypeIds::Int32),
            TTypeInfo(NTypeIds::Int32)};
        return types;
    }

    i32 id1;
    i32 value;
    i32 version;

    bool operator==(const TDataRow& r) const {
        return (id1 == r.id1) && (value == r.value) && (version == r.version);
    }

    static std::shared_ptr<arrow::Schema> MakeFullSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("id1", arrow::int32(), false),
            arrow::field("value", arrow::int32(), false),
            arrow::field("version", arrow::int32(), false)};

        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::shared_ptr<arrow::Schema> MakeDataSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("id1", arrow::int32(), false),
            arrow::field("value", arrow::int32(), false)};

        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::shared_ptr<arrow::Schema> MakeSortingSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("id1", arrow::int32(), false)};

        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::shared_ptr<arrow::Schema> MakeVersionSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("version", arrow::int32(), false)};

        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::vector<std::pair<TString, TTypeInfo>> MakeYdbSchema() {
        std::vector<std::pair<TString, TTypeInfo>> columns = {
            {"id1", TTypeInfo(NTypeIds::Int32)},
            {"value", TTypeInfo(NTypeIds::Int32)},
            {"version", TTypeInfo(NTypeIds::Int32)},
        };
        return columns;
    }

    NKikimr::TDbTupleRef ToDbTupleRef() const {
        static TCell Cells[3];
        Cells[0] = TCell::Make<i32>(id1);
        Cells[1] = TCell::Make<i32>(value);
        Cells[2] = TCell::Make<i32>(version);

        return NKikimr::TDbTupleRef(MakeTypeInfos(), Cells, 3);
    }

    TOwnedCellVec SerializedCells() const {
        NKikimr::TDbTupleRef value = ToDbTupleRef();
        std::vector<TCell> cells(value.Cells().data(), value.Cells().data() + value.Cells().size());

        return TOwnedCellVec(cells);
    }

    static std::vector<std::string> GetVersionColumns() {
        return {"version"};
    }

    static std::vector<std::string> GetSortingColumns() {
        return {"id1"};
    }
};

class TDataRowTableBuilder {
public:
    void AddRow(const TDataRow& row) {
        UNIT_ASSERT(Bid1.Append(row.id1).ok());
        UNIT_ASSERT(Bvalue.Append(row.value).ok());
        UNIT_ASSERT(Bversion.Append(row.version).ok());
    }

    std::shared_ptr<arrow::Table> Finish() {
        std::shared_ptr<arrow::Int32Array> arid1;
        std::shared_ptr<arrow::Int32Array> arvalue;
        std::shared_ptr<arrow::Int32Array> arversion;

        UNIT_ASSERT(Bid1.Finish(&arid1).ok());
        UNIT_ASSERT(Bvalue.Finish(&arvalue).ok());
        UNIT_ASSERT(Bversion.Finish(&arversion).ok());

        std::shared_ptr<arrow::Schema> schema = TDataRow::MakeFullSchema();
        return arrow::Table::Make(schema, {arid1, arvalue, arversion});
    }

    static std::shared_ptr<arrow::Table> Build(const std::vector<struct TDataRow>& rows) {
        TDataRowTableBuilder builder;
        for (const TDataRow& row : rows) {
            builder.AddRow(row);
        }
        return builder.Finish();
    }

    static std::shared_ptr<arrow::RecordBatch> buildBatch(const std::vector<std::pair<int, int>>& rows, int version) {
        TDataRowTableBuilder builder;
        for (auto [i, j] : rows) {
            builder.AddRow(TDataRow{i, j, version});
        }

        auto table = builder.Finish();
        auto schema = table->schema();
        auto tres = table->SelectColumns(std::vector<int>{
            schema->GetFieldIndex("id1"),
            schema->GetFieldIndex("value"),
            schema->GetFieldIndex("version")});
        UNIT_ASSERT(tres.ok());

        return ExtractBatch(*tres);
    };

private:
    arrow::Int32Builder Bid1;
    arrow::Int32Builder Bvalue;
    arrow::Int32Builder Bversion;
};

} // namespace

Y_UNIT_TEST_SUITE(SortableBatchPosition) {
    Y_UNIT_TEST(LogicalTypesSurviveCursorAndTrim) {
        const TTypeInfo decimalType(NScheme::TDecimalType(22, 9));
        const TTypeInfo uuidType(NTypeIds::Uuid);
        const auto schema =
            arrow::schema({ arrow::field("decimal", arrow::fixed_size_binary(16)), arrow::field("uuid", arrow::fixed_size_binary(16)) });
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        std::string decimal256(16, '\0');
        decimal256[1] = 1;
        std::string decimal1(16, '\0');
        decimal1[0] = 1;
        UNIT_ASSERT(builder.Append(decimal256).ok());
        UNIT_ASSERT(builder.Append(decimal1).ok());
        std::shared_ptr<arrow::Array> values;
        UNIT_ASSERT(builder.Finish(&values).ok());
        auto batch = arrow::RecordBatch::Make(schema, 2, { values, values });

        auto cursor = [&]() {
            std::vector<TTypeInfo> types = { decimalType, uuidType };
            NMerger::TSortableBatchPosition position(batch, 0, false, types);
            auto copy = position.BuildRWPosition(false, true);
            auto prefix = copy.TrimSortingKeys(1);
            UNIT_ASSERT(prefix.GetSorting()->GetLogicalTypes() == std::vector<TTypeInfo>{ decimalType });
            UNIT_ASSERT(prefix.ComparePartial(position) == std::partial_ordering::equivalent);
            UNIT_ASSERT(copy.GetSorting()->GetLogicalTypes() == types);
            return copy.BuildSortingCursor();
        }();

        UNIT_ASSERT(cursor.GetLogicalTypes() == (std::vector<TTypeInfo>{ decimalType, uuidType }));
        NMerger::TSortableBatchPosition one(batch, 1, false, { decimalType, uuidType });
        // The logical type is retained, while Decimal still uses its existing byte order.
        UNIT_ASSERT(cursor.Compare(*one.GetSorting(), 1) == std::partial_ordering::less);
    }

    Y_UNIT_TEST(LogicalTypesAcrossChunkBoundaries) {
        const std::vector<TTypeInfo> types = { TTypeInfo(NTypeIds::Datetime), TTypeInfo(NTypeIds::Uint32) };
        arrow::UInt32Builder builder;
        UNIT_ASSERT(builder.AppendValues({ 0, 1, 256, 257 }).ok());
        std::shared_ptr<arrow::Array> values;
        UNIT_ASSERT(builder.Finish(&values).ok());
        auto schema = arrow::schema({ arrow::field("datetime", arrow::uint32()), arrow::field("number", arrow::uint32()) });
        auto first = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{ values->Slice(0, 2), values->Slice(2, 2) });
        auto second = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{ values->Slice(0, 1), values->Slice(1, 3) });
        auto table = arrow::Table::Make(schema, { first, second });
        NMerger::TSortableScanData atZero(0, table, schema->field_names(), types);
        NMerger::TSortableScanData atOne(1, table, schema->field_names(), types);

        UNIT_ASSERT(atZero.Compare(0, atOne, 1) == std::partial_ordering::less);
        UNIT_ASSERT(atZero.Compare(0, atOne, 2) == std::partial_ordering::less);
        UNIT_ASSERT(atZero.Compare(2, atOne, 1) == std::partial_ordering::greater);
        UNIT_ASSERT(atZero.Compare(1, atOne, 1) == std::partial_ordering::equivalent);
        auto cursor = atZero.BuildCursor(2);
        UNIT_ASSERT(cursor.GetLogicalTypes() == types);
        UNIT_ASSERT(cursor.Compare(atOne, 1) == std::partial_ordering::greater);
        auto copy = atOne.BuildCopy();
        UNIT_ASSERT(copy->Compare(1, atOne, 1) == std::partial_ordering::equivalent);
    }

    Y_UNIT_TEST(FindPosition) {
        std::shared_ptr<arrow::RecordBatch> data;
        std::shared_ptr<arrow::Schema> schema =
            std::make_shared<arrow::Schema>(arrow::Schema({ std::make_shared<arrow::Field>("class", std::make_shared<arrow::StringType>()),
                std::make_shared<arrow::Field>("name", std::make_shared<arrow::StringType>()) }));
        {
            std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder;
            UNIT_ASSERT(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder).ok());

            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("c").ok());

            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());

            UNIT_ASSERT(batchBuilder->Flush(&data).ok());
        }

        std::shared_ptr<arrow::RecordBatch> search;
        {
            std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder;
            UNIT_ASSERT(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder).ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(0)->Append("a").ok());
            UNIT_ASSERT(batchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append("c").ok());
            UNIT_ASSERT(batchBuilder->Flush(&search).ok());
        }

        NMerger::TSortableBatchPosition searchPosition(search, 0, false, { TTypeInfo(NTypeIds::Utf8), TTypeInfo(NTypeIds::Utf8) });
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPosition, false, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 2);
        }

        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPosition, true, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 4);
        }

        NMerger::TSortableBatchPosition searchPositionReverse(search, 0, true, { TTypeInfo(NTypeIds::Utf8), TTypeInfo(NTypeIds::Utf8) });
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPositionReverse, false, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 3);
        }
        {
            auto findPosition = NMerger::TSortableBatchPosition::FindBound(data, searchPositionReverse, true, std::nullopt);
            UNIT_ASSERT(!!findPosition);
            UNIT_ASSERT_VALUES_EQUAL(findPosition->GetPosition(), 1);
        }
    }

    Y_UNIT_TEST(MergingSortedInputStreamReversedWithOneSearchPoint) {
        const bool isReverse = true;
        const bool deepCopy = false;
        const bool includeFinish = true;
        const bool includeStart = true;

        const int p1 = 1111;
        const int p2 = 2222;
        const int oldValue = 7777;
        const int newValue = 8888;
        const int oldVersion = 0;
        const int newVersion = 1;

        std::shared_ptr<arrow::RecordBatch> batch1 = TDataRowTableBuilder::buildBatch({{p1, oldValue}}, oldVersion);
        std::shared_ptr<arrow::RecordBatch> batch2 = TDataRowTableBuilder::buildBatch({{p2, oldValue}}, oldVersion);
        std::shared_ptr<arrow::RecordBatch> batch3 = TDataRowTableBuilder::buildBatch({{p1, newValue}, {p2, newValue}}, newVersion);

        auto vColumns = TDataRow::GetVersionColumns();
        auto sColumns = TDataRow::GetSortingColumns();

        auto merger = std::make_shared<NArrow::NMerger::TMergePartialStream>(TDataRow::MakeSortingSchema(), TDataRow::MakeDataSchema(),
            isReverse, vColumns, std::nullopt, std::nullopt, std::vector<TTypeInfo>{ TTypeInfo(NTypeIds::Int32) },
            std::vector<TTypeInfo>{ TTypeInfo(NTypeIds::Int32) });

        merger->AddSource(batch1, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        merger->AddSource(batch2, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        merger->AddSource(batch3, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        // Range to include in result batch
        NArrow::NMerger::TSortableBatchPosition startingPoint(batch1, 0, sColumns, {}, isReverse, { TTypeInfo(NTypeIds::Int32) });
        NArrow::NMerger::TSortableBatchPosition finishPoint(batch1, 0, sColumns, {}, isReverse, { TTypeInfo(NTypeIds::Int32) });

        merger->PutControlPoint(finishPoint, deepCopy);
        merger->SkipToBound(startingPoint, includeStart);

        NArrow::NMerger::TRecordBatchBuilder builder(TDataRow::MakeDataSchema()->fields());
        std::optional<NArrow::NMerger::TCursor> lastResultPosition;

        merger->DrainToControlPoint(builder, includeFinish, &lastResultPosition);

        UNIT_ASSERT(lastResultPosition);
        auto lrpVal = std::static_pointer_cast<arrow::Int32Array>(lastResultPosition->ExtractSortingPosition(TDataRow::MakeSortingSchema()->fields())->column(0))->Value(0);
        UNIT_ASSERT_EQUAL(p1, lrpVal);

        auto resultBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({builder.Finalize()}));
        UNIT_ASSERT(resultBatch);

        UNIT_ASSERT_EQUAL(1, resultBatch->num_rows());

        auto id1Col = resultBatch->GetColumnByName("id1");
        auto valueCol = resultBatch->GetColumnByName("value");

        UNIT_ASSERT_EQUAL(1, id1Col->num_chunks());
        UNIT_ASSERT_EQUAL(1, valueCol->num_chunks());

        auto id1Val = std::static_pointer_cast<arrow::Int32Array>(id1Col->chunk(0))->Value(0);
        auto valueVal = std::static_pointer_cast<arrow::Int32Array>(valueCol->chunk(0))->Value(0);

        UNIT_ASSERT_EQUAL(p1, id1Val);
        UNIT_ASSERT_EQUAL(newValue, valueVal);
    }

    Y_UNIT_TEST(MergingSortedInputStreamReversedWithRangeSearch) {
        const bool isReverse = true;
        const bool deepCopy = false;
        const bool includeFinish = true;
        const bool includeStart = true;

        const int p1 = 1111;
        const int p2 = 2222;
        const int p3 = 3333;
        const int p4 = 4444;
        const int oldValue = 7777;
        const int newValue = 8888;
        const int v0 = 0;
        const int v1 = 1;
        const int v2 = 2;

        std::shared_ptr<arrow::RecordBatch> batch1 = TDataRowTableBuilder::buildBatch({{p1, oldValue}, {p4, oldValue}}, v0);
        std::shared_ptr<arrow::RecordBatch> batch2 = TDataRowTableBuilder::buildBatch({{p2, oldValue}, {p3, oldValue}}, v1);
        std::shared_ptr<arrow::RecordBatch> batch3 = TDataRowTableBuilder::buildBatch({{p1, newValue}, {p2, newValue}, {p3, newValue}, {p4, newValue}}, v2);

        auto vColumns = TDataRow::GetVersionColumns();
        auto sColumns = TDataRow::GetSortingColumns();

        auto merger = std::make_shared<NArrow::NMerger::TMergePartialStream>(TDataRow::MakeSortingSchema(), TDataRow::MakeDataSchema(),
            isReverse, vColumns, std::nullopt, std::nullopt, std::vector<TTypeInfo>{ TTypeInfo(NTypeIds::Int32) },
            std::vector<TTypeInfo>{ TTypeInfo(NTypeIds::Int32) });

        merger->AddSource(batch1, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        merger->AddSource(batch2, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        merger->AddSource(batch3, nullptr, NArrow::NMerger::TIterationOrder(isReverse, 0));
        // Range to include in result batch, only points p2 p3 matters here
        std::shared_ptr<arrow::RecordBatch> p2sp = TDataRowTableBuilder::buildBatch({{p2, oldValue}}, v0);
        std::shared_ptr<arrow::RecordBatch> p3sp = TDataRowTableBuilder::buildBatch({{p3, oldValue}}, v0);
        NArrow::NMerger::TSortableBatchPosition startingPoint(
            isReverse ? p3sp : p2sp, 0, sColumns, {}, isReverse, { TTypeInfo(NTypeIds::Int32) });
        NArrow::NMerger::TSortableBatchPosition finishPoint(isReverse ? p2sp : p3sp, 0, sColumns, {}, isReverse, { TTypeInfo(NTypeIds::Int32) });

        merger->PutControlPoint(finishPoint, deepCopy);
        merger->SkipToBound(startingPoint, includeStart);

        NArrow::NMerger::TRecordBatchBuilder builder(TDataRow::MakeDataSchema()->fields());
        std::optional<NArrow::NMerger::TCursor> lastResultPosition;

        merger->DrainToControlPoint(builder, includeFinish, &lastResultPosition);

        UNIT_ASSERT(lastResultPosition);
        auto lrpVal = std::static_pointer_cast<arrow::Int32Array>(lastResultPosition->ExtractSortingPosition(TDataRow::MakeSortingSchema()->fields())->column(0))->Value(0);
        UNIT_ASSERT_EQUAL(p2, lrpVal);

        auto resultBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({builder.Finalize()}));
        UNIT_ASSERT(resultBatch);
        UNIT_ASSERT_EQUAL(2, resultBatch->num_rows());

        auto id1Col = resultBatch->GetColumnByName("id1");
        auto valueCol = resultBatch->GetColumnByName("value");

        UNIT_ASSERT_EQUAL(1, id1Col->num_chunks());
        UNIT_ASSERT_EQUAL(1, valueCol->num_chunks());

        auto firstId1 = std::static_pointer_cast<arrow::Int32Array>(id1Col->chunk(0))->Value(0);
        auto secondId1 = std::static_pointer_cast<arrow::Int32Array>(id1Col->chunk(0))->Value(1);
        auto firstVal = std::static_pointer_cast<arrow::Int32Array>(valueCol->chunk(0))->Value(0);
        auto secondVal = std::static_pointer_cast<arrow::Int32Array>(valueCol->chunk(0))->Value(1);

        UNIT_ASSERT_EQUAL((isReverse ? p3 : p2), firstId1);
        UNIT_ASSERT_EQUAL((isReverse ? p2 : p3), secondId1);
        UNIT_ASSERT_EQUAL(newValue, firstVal);
        UNIT_ASSERT_EQUAL(newValue, secondVal);
    }
}

}   // namespace NKikimr::NArrow
