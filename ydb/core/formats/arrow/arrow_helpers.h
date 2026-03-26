#pragma once
#include "process_columns.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/json/writer/json_value.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/util/compression.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

namespace NKikimr::NArrow {

template <class TArray, class TValue>
std::optional<ui32> FindUpperOrEqualPosition(const TArray& arr, const TValue val, const ui32 startIndex = 0) {
    if (!arr.length()) {
        return std::nullopt;
    }
    TValue left = arr.Value(startIndex);
    TValue right = arr.Value(arr.length() - 1);
    if (val < left) {
        return startIndex;
    } else if (right < val) {
        return std::nullopt;
    } else if (val == left) {
        return startIndex;
    }
    ui32 idxLeft = 0;
    ui32 idxRight = arr.length() - 1;
    while (idxRight - idxLeft > 1) {
        const ui32 idxMiddle = 0.5 * (idxRight + idxLeft);
        Y_ABORT_UNLESS(idxMiddle != idxRight);
        Y_ABORT_UNLESS(idxMiddle != idxLeft);
        const TValue middle = arr.Value(idxMiddle);
        if (middle < val) {
            idxLeft = idxMiddle;
        } else if (val < middle) {
            idxRight = idxMiddle;
        } else {
            idxRight = idxMiddle;
        }
    }
    while (idxRight && arr.Value(idxRight) == arr.Value(idxRight - 1)) {
        --idxRight;
    }
    return idxRight;
}
arrow20::Result<std::shared_ptr<arrow20::DataType>> GetArrowType(NScheme::TTypeInfo typeInfo);
arrow20::Result<std::shared_ptr<arrow20::DataType>> GetCSVArrowType(NScheme::TTypeInfo typeId);

arrow20::Result<arrow20::FieldVector> MakeArrowFields(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbColumns,  const std::set<std::string>& notNullColumns = {});
arrow20::Result<std::shared_ptr<arrow20::Schema>> MakeArrowSchema(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbColumns, const std::set<std::string>& notNullColumns = {});

std::shared_ptr<arrow20::Schema> DeserializeSchema(const TString& str);

TString SerializeBatch(const std::shared_ptr<arrow20::RecordBatch>& batch, const arrow20::ipc::IpcWriteOptions& options);
TString SerializeBatchNoCompression(const std::shared_ptr<arrow20::RecordBatch>& batch);

std::shared_ptr<arrow20::RecordBatch> DeserializeBatch(const TString& blob,
                                                     const std::shared_ptr<arrow20::Schema>& schema);

std::shared_ptr<arrow20::RecordBatch> SortBatch(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const std::shared_ptr<arrow20::Schema>& sortingKey, const bool andUnique);
std::shared_ptr<arrow20::RecordBatch> SortBatch(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const std::vector<std::shared_ptr<arrow20::Array>>& sortingKey, const bool andUnique);
bool IsSorted(const std::shared_ptr<arrow20::RecordBatch>& batch,
    const std::shared_ptr<arrow20::Schema>& sortingKey,
    bool desc = false);
bool IsSortedAndUnique(const std::shared_ptr<arrow20::RecordBatch>& batch,
                       const std::shared_ptr<arrow20::Schema>& sortingKey,
                       bool desc = false);
void DedupSortedBatch(const std::shared_ptr<arrow20::RecordBatch>& batch,
                       const std::shared_ptr<arrow20::Schema>& sortingKey,
                       std::vector<std::shared_ptr<arrow20::RecordBatch>>& out);

[[nodiscard]] std::shared_ptr<arrow20::RecordBatch> ReallocateBatch(std::shared_ptr<arrow20::RecordBatch> original);
[[nodiscard]] std::shared_ptr<arrow20::Table> ReallocateBatch(
    const std::shared_ptr<arrow20::Table>& original, arrow20::MemoryPool* pool = arrow20::default_memory_pool());
[[nodiscard]] std::shared_ptr<arrow20::ChunkedArray> ReallocateArray(
    const std::shared_ptr<arrow20::ChunkedArray>& original, arrow20::MemoryPool* pool = arrow20::default_memory_pool());
[[nodiscard]] std::shared_ptr<arrow20::Array> ReallocateArray(const std::shared_ptr<arrow20::Array>& arr, arrow20::MemoryPool* pool = arrow20::default_memory_pool());

std::vector<std::shared_ptr<arrow20::Field>> BuildFakeFields(const std::vector<std::shared_ptr<arrow20::Array>>& columns);

std::shared_ptr<arrow20::Schema> BuildFakeSchema(const std::vector<std::shared_ptr<arrow20::Array>>& columns);


}
