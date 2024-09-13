#include "arrow/status.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NArrow;

void CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(std::shared_ptr<arrow::RecordBatch> batch, ui32 countRows) {
    TBatchSplitttingContext context(8 * 1024 * 1024);

    TRowSizeCalculator calculator(8);
    calculator.InitBatch(batch);
    for (ui32 i = 0; i < batch->num_rows(); i += countRows) {
        ui32 serializedBatchSize = 0;
        ui32 batchSize = 0;
        if (batch->num_rows() - i > countRows) {
            auto sliceBatch = batch->Slice(i, countRows);
            serializedBatchSize = TSerializedBatch::Build(sliceBatch, context).GetSize();
            batchSize = calculator.GetSliceSize(i, countRows);
        } else {
            auto sliceBatch = batch->Slice(i, batch->num_rows() - i);
            serializedBatchSize = TSerializedBatch::Build(sliceBatch, context).GetSize();
            batchSize = calculator.GetSliceSize(i, batch->num_rows() - i);
        }
        UNIT_ASSERT_EQUAL(serializedBatchSize, batchSize);
    }
}

void CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(std::shared_ptr<arrow::RecordBatch> batch, ui32 countParts) {
    CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(batch, batch->num_rows() / countParts);
}

template <typename Builder>
std::shared_ptr<arrow::Array> createIntegerColumnWithoutNull(ui32 numRows) {
    Builder builder;
    arrow::Status status;
    for (ui32 i = 0; i < numRows; i++) {
        status = builder.Append(1);
        if (!status.ok()) {
            return {};
        }
    }
    std::shared_ptr<arrow::Array> result;
    status = builder.Finish(&result);
    if (!status.ok()) {
        return {};
    }
    return result;
}

template <typename Builder>
std::shared_ptr<arrow::Array> createIntegerColumnWithNull(ui32 numRows) {
    Builder builder;
    arrow::Status status;
    for (ui32 i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            status = builder.Append(1);
        } else {
            status = builder.AppendNull();
        }
        if (!status.ok()) {
            return {};
        }
    }
    std::shared_ptr<arrow::Array> result;
    status = builder.Finish(&result);
    if (!status.ok()) {
        return {};
    }
    return result;
}

std::shared_ptr<arrow::Array> createStringColumnWithoutNull(ui32 numRows) {
    arrow::StringBuilder builder;
    arrow::Status status;
    for (ui32 i = 0; i < numRows; i++) {
        status = builder.Append("A" + std::to_string(i));
        if (!status.ok()) {
            return {};
        }
    }
    std::shared_ptr<arrow::Array> result;
    status = builder.Finish(&result);
    if (!status.ok()) {
        return {};
    }
    return result;
}

std::shared_ptr<arrow::Array> createStringColumnWithNull(ui32 numRows) {
    arrow::StringBuilder builder;
    arrow::Status status;
    for (ui32 i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            status = builder.Append("A" + std::to_string(i));
        } else {
            status = builder.AppendNull();
        }
        if (!status.ok()) {
            return {};
        }
    }
    std::shared_ptr<arrow::Array> result;
    status = builder.Finish(&result);
    if (!status.ok()) {
        return {};
    }
    return result;
}

Y_UNIT_TEST_SUITE(RowSizeCalculator) {
    Y_UNIT_TEST(Int8) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int8Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int8", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(Int8WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::Int8Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int8", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(Int16) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int16Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int16", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(Int16WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::Int16Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int16", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt32) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt32", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt32", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(Int32) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(Int32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.front()->type(), true));
        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64Int32) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64WithNullInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64NInt32) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64Int32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("Int32", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64WithNullTwoInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 2; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64TwoInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 2; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64HundredInt32) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 100; i++) {
            columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64WithNullHundredInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 100; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64TenThousandInt32) {
        ui64 numRows = 10000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithoutNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 10'000; i++) {
            columns.emplace_back(createIntegerColumnWithoutNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(batch, 3334);
        CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(batch, 3333);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(UInt64WithNullTenThousandInt32WithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("UInt64", columns.back()->type(), true));

        for (ui32 i = 0; i < 10'000; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(batch, 3334);
        CompareSizeSerializedFullBatchWithSizeCalculatorByCountRows(batch, 3333);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(IntegerBatchWithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        for (ui32 i = 0; i < 2'500; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int8Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int8" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::Int16Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int16" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int64", columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(String) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createStringColumnWithoutNull(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("StringColumn", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(TwoString) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        for (ui32 i = 0; i < 2; i++) {
            columns.emplace_back(createStringColumnWithoutNull(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("StringColumn" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(TwoStringWithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        for (ui32 i = 0; i < 2; i++) {
            columns.emplace_back(createStringColumnWithNull(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("StringColumn" + std::to_string(i), columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(TwoStringWithNullAndWithout) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        columns.emplace_back(createStringColumnWithoutNull(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("StringColumn", columns.back()->type(), true));

        columns.emplace_back(createStringColumnWithNull(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>("StringColumn0", columns.back()->type(), true));

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }

    Y_UNIT_TEST(IntegerStringBatchWithNull) {
        ui64 numRows = 1000;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::shared_ptr<arrow::Schema> schema;

        for (ui32 i = 0; i < 2'000; i++) {
            columns.emplace_back(createIntegerColumnWithNull<arrow::Int8Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int8" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createStringColumnWithoutNull(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("StringColumn" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::Int16Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int16" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::Int32Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int32" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createStringColumnWithoutNull(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("StringColumn0" + std::to_string(i), columns.back()->type(), true));

            columns.emplace_back(createIntegerColumnWithNull<arrow::UInt64Builder>(numRows));
            fields.emplace_back(std::make_shared<arrow::Field>("Int64", columns.back()->type(), true));
        }

        auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);

        for (ui32 i = 1; i <= 10; i++) {
            CompareSizeSerializedFullBatchWithSizeCalculatorByCountParts(batch, i);
        }
    }
}
