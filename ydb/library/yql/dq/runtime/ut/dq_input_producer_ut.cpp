#include <ydb/library/yql/dq/runtime/dq_async_input.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NDq;

namespace {

// Two sort keys and a payload identifying the row independently of its keys.
using TRow = std::array<i64, 3>;

struct TMergeTest {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TMultiType* InputType;
    TInstant StartTs;
    ui64 InputsConsumed = 0;
    TVector<IDqAsyncInputBuffer::TPtr> Inputs;
    NUdf::TUnboxedValue Merge;

    explicit TMergeTest(ui32 inputCount, bool ascending = true)
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("TestInputMerge")
        , HolderFactory(Alloc.Ref(), MemInfo)
    {
        auto itemType = TDataType::Create(NUdf::TDataType<i64>::Id, TypeEnv);
        auto blockType = TBlockType::Create(itemType, TBlockType::EShape::Many, TypeEnv);
        auto lengthType = TBlockType::Create(TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv),
            TBlockType::EShape::Scalar, TypeEnv);
        TType* columns[] = {blockType, blockType, blockType, lengthType};
        InputType = TMultiType::Create(std::size(columns), columns, TypeEnv);

        TVector<TSortColumnInfo> sortCols;
        for (ui32 column = 0; column < 2; ++column) {
            TSortColumnInfo sortCol{TColumnInfo("key", column, itemType, false)};
            sortCol.Ascending = column == 0 ? ascending : !ascending;
            sortCols.push_back(std::move(sortCol));
        }

        TVector<IDqInput::TPtr> inputs;
        for (ui32 i = 0; i < inputCount; ++i) {
            Inputs.push_back(CreateDqAsyncInputBuffer(i, "test", InputType, 1 << 20, TCollectStatsLevel::None));
            inputs.push_back(Inputs.back());
        }
        Merge = CreateInputMergeValue(InputType, std::move(inputs), std::move(sortCols),
            HolderFactory, {}, StartTs, InputsConsumed);
    }

    void PushBlock(ui32 input, const TVector<TRow>& rows) {
        std::array<NUdf::TUnboxedValue, 4> values;
        for (ui32 column = 0; column < 3; ++column) {
            arrow::Int64Builder builder;
            for (const auto& row : rows) {
                UNIT_ASSERT(builder.Append(row[column]).ok());
            }
            std::shared_ptr<arrow::Array> array;
            UNIT_ASSERT(builder.Finish(&array).ok());
            values[column] = HolderFactory.CreateArrowBlock(arrow::Datum(array), DefaultDatumTestValidationMode);
        }
        values.back() = HolderFactory.CreateArrowBlock(
            arrow::Datum(std::make_shared<arrow::UInt64Scalar>(rows.size())), DefaultDatumTestValidationMode);
        TUnboxedValueBatch batch(InputType);
        batch.PushRow(values.data(), values.size());
        Inputs[input]->Push(std::move(batch), rows.size() * sizeof(TRow));
    }

    void FinishInputs() {
        for (auto& input : Inputs) {
            input->Finish();
        }
    }

    TVector<TRow> ReadAll() {
        TVector<TRow> rows;
        for (;;) {
            std::array<NUdf::TUnboxedValue, 4> values;
            const auto status = Merge.WideFetch(values.data(), values.size());
            if (status == NUdf::EFetchStatus::Finish) {
                return rows;
            }
            UNIT_ASSERT_C(status == NUdf::EFetchStatus::Ok, "Unexpected Yield");
            const ui64 length = TArrowBlock::From(values.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
            UNIT_ASSERT(length > 0);
            std::array<std::shared_ptr<arrow::Int64Array>, 3> columns;
            for (ui32 column = 0; column < columns.size(); ++column) {
                columns[column] = TArrowBlock::From(values[column]).GetDatum().array_as<arrow::Int64Array>();
                UNIT_ASSERT_VALUES_EQUAL(columns[column]->length(), length);
            }
            for (ui64 row = 0; row < length; ++row) {
                rows.push_back({columns[0]->Value(row), columns[1]->Value(row), columns[2]->Value(row)});
            }
        }
    }

    void AssertRows(const TVector<TRow>& expected) {
        const auto actual = ReadAll();
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (size_t row = 0; row < expected.size(); ++row) {
            for (size_t column = 0; column < expected[row].size(); ++column) {
                UNIT_ASSERT_VALUES_EQUAL_C(actual[row][column], expected[row][column],
                    "row " << row << ", column " << column);
            }
        }
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TDqInputMergeBlock) {
    Y_UNIT_TEST(MergesAscendingRunsWithStableTies) {
        TMergeTest test(3);
        test.PushBlock(0, {{1, 9, 100}, {1, 9, 101}, {1, 7, 102}, {2, 8, 103}});
        test.PushBlock(0, {{2, 8, 104}, {2, 5, 105}, {4, 1, 106}});
        test.PushBlock(1, {{1, 9, 200}, {1, 8, 201}, {1, 6, 202}, {2, 8, 203}, {3, 9, 204}, {4, 1, 205}});
        test.PushBlock(2, {{0, 0, 300}, {1, 9, 301}, {2, 8, 302}, {3, 8, 303}});
        test.FinishInputs();

        // Equal keys keep input order, including when input 0 advances to its next block.
        test.AssertRows({
            {0, 0, 300},
            {1, 9, 100}, {1, 9, 101}, {1, 9, 200}, {1, 9, 301},
            {1, 8, 201}, {1, 7, 102}, {1, 6, 202},
            {2, 8, 103}, {2, 8, 104}, {2, 8, 203}, {2, 8, 302}, {2, 5, 105},
            {3, 9, 204}, {3, 8, 303}, {4, 1, 106}, {4, 1, 205},
        });
    }

    Y_UNIT_TEST(MergesDescendingRunsWithStableTies) {
        TMergeTest test(2, false);
        test.PushBlock(0, {{3, 1, 100}, {3, 1, 101}, {3, 2, 102}, {1, 0, 103}});
        test.PushBlock(1, {{3, 1, 200}, {3, 3, 201}, {2, 0, 202}});
        test.FinishInputs();

        test.AssertRows({
            {3, 1, 100}, {3, 1, 101}, {3, 1, 200}, {3, 2, 102},
            {3, 3, 201}, {2, 0, 202}, {1, 0, 103},
        });
    }

    Y_UNIT_TEST(ResumesAfterYieldBetweenBlocks) {
        TMergeTest test(2);
        test.PushBlock(0, {{1, 0, 100}, {2, 0, 101}});
        test.PushBlock(1, {{4, 0, 200}});
        test.Inputs[1]->Finish();

        std::array<NUdf::TUnboxedValue, 4> values;
        UNIT_ASSERT(test.Merge.WideFetch(values.data(), values.size()) == NUdf::EFetchStatus::Yield);

        test.PushBlock(0, {{3, 0, 102}, {5, 0, 103}});
        test.Inputs[0]->Finish();
        test.AssertRows({{1, 0, 100}, {2, 0, 101}, {3, 0, 102}, {4, 0, 200}, {5, 0, 103}});
    }

    Y_UNIT_TEST(SkipsEmptyBlocksAndInputs) {
        TMergeTest test(3);
        test.PushBlock(1, {});
        test.PushBlock(1, {{1, 0, 100}});
        test.PushBlock(1, {});
        test.FinishInputs();

        test.AssertRows({{1, 0, 100}});
    }

#ifndef NDEBUG
    Y_UNIT_TEST(RejectsUnsortedAscendingPrimaryKey) {
        TMergeTest test(2);
        test.PushBlock(1, {{1, 0, 100}, {3, 0, 101}, {2, 0, 102}, {4, 0, 103}});
        test.FinishInputs();

        UNIT_ASSERT_EXCEPTION_CONTAINS(test.ReadAll(), TYqlPanic,
            "Sort order violation on input #1 between rows 1 and 2");
    }

    Y_UNIT_TEST(RejectsUnsortedDescendingPrimaryKey) {
        TMergeTest test(2, false);
        test.PushBlock(1, {{4, 0, 100}, {2, 0, 101}, {3, 0, 102}, {1, 0, 103}});
        test.FinishInputs();

        UNIT_ASSERT_EXCEPTION_CONTAINS(test.ReadAll(), TYqlPanic,
            "Sort order violation on input #1 between rows 1 and 2");
    }

    Y_UNIT_TEST(RejectsUnsortedSecondaryKey) {
        TMergeTest test(1);
        test.PushBlock(0, {{1, 9, 100}, {1, 7, 101}, {1, 8, 102}, {2, 0, 103}});
        test.FinishInputs();

        UNIT_ASSERT_EXCEPTION_CONTAINS(test.ReadAll(), TYqlPanic,
            "Sort order violation on input #0 between rows 1 and 2");
    }

    Y_UNIT_TEST(RejectsUnsortedNextBlock) {
        TMergeTest test(1);
        test.PushBlock(0, {{1, 0, 100}, {2, 0, 101}});
        test.PushBlock(0, {{3, 0, 102}, {5, 0, 103}, {4, 0, 104}});
        test.FinishInputs();

        UNIT_ASSERT_EXCEPTION_CONTAINS(test.ReadAll(), TYqlPanic,
            "Sort order violation on input #0 between rows 1 and 2");
    }
#endif
}
