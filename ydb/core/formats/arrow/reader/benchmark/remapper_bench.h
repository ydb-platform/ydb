#pragma once
// Микробенчмарк production-пути компакшна: построение "remapper"-батчей из набора
// отсортированных источников. Сравниваются две реализации, дающие эквивалентный
// результат на одних и тех же входных данных:
//
//   1) TSortIndicesMerger::BuildRemapper  — concat + arrow_next sort_indices + take + dedup
//   2) TMergePartialStream::DrainAllParts — потоковый heap-merge (исходная реализация)
//
// см. core/tx/columnshard/engines/changes/compaction/merger.cpp:271 (ветка if/else),
// откуда оба вызова и взяты дословно.

#include "merges.h"

#include <ydb/core/formats/arrow/container/container.h>
#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/position.h>

#include <ydb/core/tx/columnshard/engines/changes/compaction/sort_merger.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <memory>
#include <vector>

namespace NRemapperBench {

using NKikimr::NArrow::TColumnFilter;
using NKikimr::NArrow::TGeneralContainer;
using NKikimr::NArrow::NMerger::TIntervalPositions;
using NKikimr::NArrow::NMerger::TIterationOrder;
using NKikimr::NArrow::NMerger::TMergePartialStream;
using NKikimr::NArrow::NMerger::TSortableBatchPosition;
using NKikimr::NOlap::IIndexInfo;
using NKikimr::NOlap::NCompaction::IColumnMerger;
using NKikimr::NOlap::NCompaction::TSortIndicesMerger;

// Ключ сортировки/замены: PK = (ts, a, b) — как replaceKey в реальном компакшне.
inline std::shared_ptr<arrow::Schema> MakeReplaceKey() {
    return arrow::schema({
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
    });
}

// Проекция результата (indexFields), которую обязаны выдать обе реализации:
// адрес источника (portion_id, portion_record_idx) + snapshot (plan_step, tx_id, write_id).
// Дословно повторяет набор полей из TMerger::Execute (без delete-flag).
inline std::vector<std::shared_ptr<arrow::Field>> MakeIndexFields() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.emplace_back(IColumnMerger::PortionIdField);
    fields.emplace_back(IColumnMerger::PortionRecordIndexField);
    IIndexInfo::AddSnapshotFields(fields);
    return fields;
}

// Превращает "сырой" источник merges.h (ts, a, b, ver:int64) в TGeneralContainer,
// какой видит merge внутри компакшна:
//   * ver  -> snapshot plan_step (uint64); tx_id = write_id = 0.
//     Версионная сортировка по (plan_step, tx_id, write_id) DESC => победителем
//     дубля PK становится строка с максимальным ver — та же семантика, что у "ver" DESC
//     в существующем MergeOnce, только через snapshot-колонки.
//   * $$__portion_id          (uint16) — индекс источника (адрес портиона).
//   * $$__portion_record_idx  (uint32) — позиция строки внутри источника.
// Эти синтетические адресные колонки в реальном пайплайне добавляет TMerger::Execute
// перед вызовом merge.
inline std::shared_ptr<TGeneralContainer> MakeRemapperSource(
        const std::shared_ptr<arrow::RecordBatch>& base, ui16 portionId) {
    const int64_t numRows = base->num_rows();
    const auto& ver = static_cast<const arrow::Int64Array&>(*base->GetColumnByName("ver"));

    arrow::UInt64Builder planStepBuilder, txIdBuilder, writeIdBuilder;
    arrow::UInt16Builder portionIdBuilder;
    arrow::UInt32Builder recordIdxBuilder;
    for (int64_t i = 0; i < numRows; ++i) {
        Y_ABORT_UNLESS(planStepBuilder.Append(static_cast<uint64_t>(ver.Value(i))).ok());
        Y_ABORT_UNLESS(txIdBuilder.Append(0).ok());
        Y_ABORT_UNLESS(writeIdBuilder.Append(0).ok());
        Y_ABORT_UNLESS(portionIdBuilder.Append(portionId).ok());
        Y_ABORT_UNLESS(recordIdxBuilder.Append(static_cast<uint32_t>(i)).ok());
    }

    std::shared_ptr<arrow::Array> planStep, txId, writeId, portionIdArr, recordIdx;
    Y_ABORT_UNLESS(planStepBuilder.Finish(&planStep).ok());
    Y_ABORT_UNLESS(txIdBuilder.Finish(&txId).ok());
    Y_ABORT_UNLESS(writeIdBuilder.Finish(&writeId).ok());
    Y_ABORT_UNLESS(portionIdBuilder.Finish(&portionIdArr).ok());
    Y_ABORT_UNLESS(recordIdxBuilder.Finish(&recordIdx).ok());

    auto schema = arrow::schema({
        base->schema()->field(0),   // ts
        base->schema()->field(1),   // a
        base->schema()->field(2),   // b
        IIndexInfo::PlanStepField,
        IIndexInfo::TxIdField,
        IIndexInfo::WriteIdField,
        IColumnMerger::PortionIdField,
        IColumnMerger::PortionRecordIndexField,
    });
    auto batch = arrow::RecordBatch::Make(schema, numRows,
        {base->column(0), base->column(1), base->column(2),
         planStep, txId, writeId, portionIdArr, recordIdx});
    return std::make_shared<TGeneralContainer>(batch);
}

// Строит `count` checkpoint-позиций, режущих выход merge на ~count+1 частей — как
// бакет-границы оптимайзера в реальном компакшне. Опорный батч `ref` (один источник)
// строго возрастает по PK (ts = i*1000), поэтому равномерно разнесённые позиции дают
// строго возрастающие, различные ключи — то, что требует TIntervalPositions::AddPosition.
// Cut-ключи сравниваются с PK-колонками (ts,a,b) выходного батча в обеих реализациях.
inline TIntervalPositions MakeCheckPoints(const std::shared_ptr<arrow::RecordBatch>& ref, ui32 count) {
    TIntervalPositions positions;
    const i64 numRows = ref->num_rows();
    static const std::vector<std::string> pkColumns = { "ts", "a", "b" };
    i64 prev = -1;
    for (ui32 k = 1; k <= count; ++k) {
        i64 pos = static_cast<i64>(static_cast<double>(k) / (count + 1) * numRows);
        pos = std::min<i64>(std::max<i64>(pos, 0), numRows - 1);
        if (pos <= prev) {
            continue;   // защита от совпадающих позиций на маленьких ref
        }
        prev = pos;
        positions.AddPosition(
            TSortableBatchPosition(ref, static_cast<ui32>(pos), pkColumns, {}, /*reverseSort=*/false),
            /*includePositionToLeftInterval=*/false);
    }
    return positions;
}

// Фикстура для remapper-бенчмарка: переиспользует генерацию данных merges.h
// (TFixture: N источников по 10k строк с межисточниковыми дубликатами PK),
// оборачивая каждый источник в TGeneralContainer со snapshot+адресными колонками.
struct TRemapperFixture {
    std::vector<std::shared_ptr<TGeneralContainer>> Batches;
    std::vector<std::shared_ptr<TColumnFilter>> Filters;   // nullptr == total-allow
    std::shared_ptr<arrow::Schema> ReplaceKey = MakeReplaceKey();
    std::vector<std::string> SnapshotColumnNames = IIndexInfo::GetSnapshotColumnNames();
    std::vector<std::shared_ptr<arrow::Field>> IndexFields = MakeIndexFields();
    std::shared_ptr<arrow::Schema> DataSchema = std::make_shared<arrow::Schema>(IndexFields);
    TIntervalPositions CheckPoints;   // count == число входных батчей

    explicit TRemapperFixture(const TFixture& base) {
        Batches.reserve(base.Batches.size());
        Filters.reserve(base.Batches.size());
        ui16 portionId = 0;
        for (auto&& b : base.Batches) {
            Batches.push_back(MakeRemapperSource(b, portionId++));
            Filters.push_back(nullptr);
        }
        // base.Batches[0] не пополняется дубликатами (см. TFixture) => строго возрастает по PK.
        CheckPoints = MakeCheckPoints(base.Batches.front(), base.Batches.size());
    }
};

// --- реализация 1: arrow_next sort_indices + take + dedup ---
inline std::vector<std::shared_ptr<arrow::RecordBatch>> RunBuildRemapper(const TRemapperFixture& f) {
    return TSortIndicesMerger::BuildRemapper(
        f.Batches, f.Filters, f.ReplaceKey, f.SnapshotColumnNames, f.IndexFields, f.CheckPoints);
}

// --- реализация 2: потоковый heap-merge ---
inline std::vector<std::shared_ptr<arrow::RecordBatch>> RunDrainAllParts(const TRemapperFixture& f) {
    TMergePartialStream merger(
        f.ReplaceKey, f.DataSchema, /*reverse=*/false, f.SnapshotColumnNames, std::nullopt, std::nullopt);
    ui32 idx = 0;
    for (auto&& b : f.Batches) {
        merger.AddSource(b, f.Filters[idx], TIterationOrder::Forward(0));
        ++idx;
    }
    return merger.DrainAllParts(f.CheckPoints, f.IndexFields);
}

// Function-local статики: пул Arrow-памяти должен быть проинициализирован до билдеров.
inline const TRemapperFixture& GetRemap_2src()  { static TRemapperFixture f{ Get_2src_10k() };  return f; }
inline const TRemapperFixture& GetRemap_5src()  { static TRemapperFixture f{ Get_5src_10k() };  return f; }
inline const TRemapperFixture& GetRemap_10src() { static TRemapperFixture f{ Get_10src_10k() }; return f; }
inline const TRemapperFixture& GetRemap_20src() { static TRemapperFixture f{ Get_20src_10k() }; return f; }

}   // namespace NRemapperBench
