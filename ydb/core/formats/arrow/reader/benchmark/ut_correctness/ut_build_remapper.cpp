#include <library/cpp/testing/unittest/registar.h>

#include "../remapper_bench.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/json_simple.h>

#include <ydb/core/base/backtrace.h>

// ===========================================================================
// Юнит-тесты для TSortIndicesMerger::BuildRemapper.
//
// Все входные данные захардкожены (никакой генерации/рандома): два источника
// по 5 строк, с межисточниковыми дубликатами PK на разных версиях, чтобы
// проверить и слияние, и дедупликацию (победитель = максимальная версия), и
// проекцию выхода ровно в indexFields, и нарезку чекпойнтами, и фильтры.
//
// Сырые батчи (ts, a, b, ver) строятся через arrow::ipc::internal::json::
// ArrayFromJSON — это «JsonToBatch»-хелпер, доступный в текущей сборке Arrow
// (contrib/libs/apache/arrow/cpp/src/arrow/ipc/json_simple.h).
// ===========================================================================

Y_UNIT_TEST_SUITE(BuildRemapper) {

    using namespace NRemapperBench;

    // Один столбец из JSON-литерала. ArrayFromJSON для timestamp принимает целые
    // (raw value), для utf8 — строки, для int64 — целые.
    std::shared_ptr<arrow::Array> FromJson(const std::shared_ptr<arrow::DataType>& type, const std::string& json) {
        std::shared_ptr<arrow::Array> out;
        auto status = arrow::ipc::internal::json::ArrayFromJSON(type, json, &out);
        UNIT_ASSERT_C(status.ok(), status.ToString());
        return out;
    }

    // Сырой источник (ts, a, b, ver:int64) — ровно тот формат, который ожидает
    // MakeRemapperSource из remapper_bench.h (ver -> snapshot plan_step).
    std::shared_ptr<arrow::RecordBatch> MakeRawBatch(
            const std::string& ts, const std::string& a, const std::string& b, const std::string& ver) {
        auto schema = arrow::schema({
            arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
            arrow::field("a", arrow::utf8()),
            arrow::field("b", arrow::utf8()),
            arrow::field("ver", arrow::int64()),
        });
        auto tsArr = FromJson(schema->field(0)->type(), ts);
        auto aArr = FromJson(schema->field(1)->type(), a);
        auto bArr = FromJson(schema->field(2)->type(), b);
        auto verArr = FromJson(schema->field(3)->type(), ver);
        UNIT_ASSERT_VALUES_EQUAL(tsArr->length(), aArr->length());
        UNIT_ASSERT_VALUES_EQUAL(tsArr->length(), bArr->length());
        UNIT_ASSERT_VALUES_EQUAL(tsArr->length(), verArr->length());
        return arrow::RecordBatch::Make(schema, tsArr->length(), {tsArr, aArr, bArr, verArr});
    }

    // Ожидаемая (спроецированная в indexFields) выходная строка remapper'а.
    struct TExpectedRow {
        ui16 PortionId;
        ui32 RecordIdx;
        ui64 PlanStep;   // == ver победителя
    };

    // Склеивает все выходные батчи в один линейный список адрес+snapshot строк и
    // сверяет со списком ожидаемых строк (порядок — по PK по возрастанию).
    void AssertRows(const std::vector<std::shared_ptr<arrow::RecordBatch>>& result,
            const std::vector<TExpectedRow>& expected) {
        ui32 total = 0;
        for (auto&& batch : result) {
            total += batch->num_rows();
        }
        UNIT_ASSERT_VALUES_EQUAL_C(total, expected.size(), "total output rows");

        ui32 row = 0;
        for (auto&& batch : result) {
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 5);   // portion_id, record_idx, plan_step, tx_id, write_id
            const auto& portionId = static_cast<const arrow::UInt16Array&>(*batch->GetColumnByName("$$__portion_id"));
            const auto& recordIdx = static_cast<const arrow::UInt32Array&>(*batch->GetColumnByName("$$__portion_record_idx"));
            const auto& planStep = static_cast<const arrow::UInt64Array&>(*batch->GetColumnByName("_yql_plan_step"));
            const auto& txId = static_cast<const arrow::UInt64Array&>(*batch->GetColumnByName("_yql_tx_id"));
            const auto& writeId = static_cast<const arrow::UInt64Array&>(*batch->GetColumnByName("_yql_write_id"));
            for (int64_t i = 0; i < batch->num_rows(); ++i, ++row) {
                UNIT_ASSERT_VALUES_EQUAL_C(portionId.Value(i), expected[row].PortionId, "row " << row << " portion_id");
                UNIT_ASSERT_VALUES_EQUAL_C(recordIdx.Value(i), expected[row].RecordIdx, "row " << row << " record_idx");
                UNIT_ASSERT_VALUES_EQUAL_C(planStep.Value(i), expected[row].PlanStep, "row " << row << " plan_step");
                UNIT_ASSERT_VALUES_EQUAL_C(txId.Value(i), 0, "row " << row << " tx_id");
                UNIT_ASSERT_VALUES_EQUAL_C(writeId.Value(i), 0, "row " << row << " write_id");
            }
        }
    }

    // Два источника по 5 строк. PK = (ts, a, b), ver -> plan_step.
    //
    // source0 (portion 0):                       source1 (portion 1):
    //   idx ts    a  b  ver                         idx ts    a  b  ver
    //   0   1000  0  0  1                            0   2000  0  1  5   <- дубль PK с src0/idx1, ver выше -> победит
    //   1   2000  0  1  1                            1   3000  1  2  0   <- дубль PK с src0/idx2, ver ниже -> проиграет
    //   2   3000  1  2  1                            2   6000  3  5  1
    //   3   4000  1  3  1                            3   7000  3  6  1
    //   4   5000  2  4  1                            4   8000  4  7  1
    std::pair<std::shared_ptr<TGeneralContainer>, std::shared_ptr<TGeneralContainer>> MakeSources() {
        auto raw0 = MakeRawBatch(
            "[1000, 2000, 3000, 4000, 5000]",
            R"(["0", "0", "1", "1", "2"])",
            R"(["0", "1", "2", "3", "4"])",
            "[1, 1, 1, 1, 1]");
        auto raw1 = MakeRawBatch(
            "[2000, 3000, 6000, 7000, 8000]",
            R"(["0", "1", "3", "3", "4"])",
            R"(["1", "2", "5", "6", "7"])",
            "[5, 0, 1, 1, 1]");
        return { MakeRemapperSource(raw0, /*portionId=*/0), MakeRemapperSource(raw1, /*portionId=*/1) };
    }

    // Ожидаемый отдедупленный выход (по PK по возрастанию):
    //   PK            источник-победитель        plan_step
    //   1000,0,0      portion0 idx0              1
    //   2000,0,1      portion1 idx0 (ver 5)      5
    //   3000,1,2      portion0 idx2 (ver 1>0)    1
    //   4000,1,3      portion0 idx3              1
    //   5000,2,4      portion0 idx4              1
    //   6000,3,5      portion1 idx2              1
    //   7000,3,6      portion1 idx3              1
    //   8000,4,7      portion1 idx4              1
    std::vector<TExpectedRow> ExpectedAll() {
        return {
            { 0, 0, 1 },
            { 1, 0, 5 },
            { 0, 2, 1 },
            { 0, 3, 1 },
            { 0, 4, 1 },
            { 1, 2, 1 },
            { 1, 3, 1 },
            { 1, 4, 1 },
        };
    }

    // Базовый случай: пустые чекпойнты -> один выходной батч со всеми строками.
    Y_UNIT_TEST(TwoSourcesNoCheckpoints) {
        NKikimr::EnableYDBBacktraceFormat();
        auto [src0, src1] = MakeSources();

        std::vector<std::shared_ptr<TGeneralContainer>> batches{ src0, src1 };
        std::vector<std::shared_ptr<TColumnFilter>> filters{ nullptr, nullptr };

        auto result = TSortIndicesMerger::BuildRemapper(
            batches, filters, MakeReplaceKey(), IIndexInfo::GetSnapshotColumnNames(), MakeIndexFields(), TIntervalPositions());

        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        AssertRows(result, ExpectedAll());
    }

    // Чекпойнт на PK (5000,"2","4") режет выход на два батча: [0,4) и [4,8).
    Y_UNIT_TEST(TwoSourcesWithCheckpoint) {
        NKikimr::EnableYDBBacktraceFormat();
        auto [src0, src1] = MakeSources();

        std::vector<std::shared_ptr<TGeneralContainer>> batches{ src0, src1 };
        std::vector<std::shared_ptr<TColumnFilter>> filters{ nullptr, nullptr };

        // Опорный однострочный батч с cut-ключом (ts,a,b) = (5000,"2","4").
        auto cutRef = MakeRawBatch("[5000]", R"(["2"])", R"(["4"])", "[0]");
        TIntervalPositions checkPoints;
        checkPoints.AddPosition(
            TSortableBatchPosition(cutRef, 0, { "ts", "a", "b" }, {}, /*reverseSort=*/false),
            /*includePositionToLeftInterval=*/false);

        auto result = TSortIndicesMerger::BuildRemapper(
            batches, filters, MakeReplaceKey(), IIndexInfo::GetSnapshotColumnNames(), MakeIndexFields(), checkPoints);

        UNIT_ASSERT_VALUES_EQUAL(result.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result[0]->num_rows(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result[1]->num_rows(), 4);
        AssertRows(result, ExpectedAll());   // конкатенация батчей == полный ожидаемый порядок
    }

    // Фильтр на source1 отвергает строку idx0 — победителя PK (2000,"0","1").
    // Отклонённый победитель удаляет весь PK целиком (а не уступает место строке
    // меньшей версии из source0), поэтому в выходе остаётся 7 строк без (2000,0,1).
    Y_UNIT_TEST(RejectedWinnerDropsWholeKey) {
        NKikimr::EnableYDBBacktraceFormat();
        auto [src0, src1] = MakeSources();

        auto filter1 = std::make_shared<TColumnFilter>(TColumnFilter::BuildAllowFilter());
        filter1->Add(false, 1);   // idx0 (победитель 2000,0,1) — отвергнут
        filter1->Add(true, 4);    // idx1..idx4 — приняты

        std::vector<std::shared_ptr<TGeneralContainer>> batches{ src0, src1 };
        std::vector<std::shared_ptr<TColumnFilter>> filters{ nullptr, filter1 };

        auto result = TSortIndicesMerger::BuildRemapper(
            batches, filters, MakeReplaceKey(), IIndexInfo::GetSnapshotColumnNames(), MakeIndexFields(), TIntervalPositions());

        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        AssertRows(result, {
            { 0, 0, 1 },   // 1000,0,0
            // 2000,0,1 — победитель отвергнут фильтром, ключ выпал целиком
            { 0, 2, 1 },   // 3000,1,2
            { 0, 3, 1 },   // 4000,1,3
            { 0, 4, 1 },   // 5000,2,4
            { 1, 2, 1 },   // 6000,3,5
            { 1, 3, 1 },   // 7000,3,6
            { 1, 4, 1 },   // 8000,4,7
        });
    }

}
