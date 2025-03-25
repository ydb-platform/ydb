#pragma once
#include "process_columns.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/json/writer/json_value.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

namespace NKikimr::NArrow {

template <class TArray, class TValue>
std::optional<ui32> FindUpperOrEqualPosition(const TArray& arr, const TValue val) {
    if (!arr.length()) {
        return std::nullopt;
    }
    TValue left = arr.Value(0);
    TValue right = arr.Value(arr.length() - 1);
    if (val < left) {
        return 0;
    } else if (right < val) {
        return std::nullopt;
    } else if (val == left) {
        return 0;
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
    return idxRight;
}
arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(NScheme::TTypeInfo typeInfo);
arrow::Result<std::shared_ptr<arrow::DataType>> GetCSVArrowType(NScheme::TTypeInfo typeId);

arrow::Result<arrow::FieldVector> MakeArrowFields(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns,  const std::set<std::string>& notNullColumns = {});
arrow::Result<std::shared_ptr<arrow::Schema>> MakeArrowSchema(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, const std::set<std::string>& notNullColumns = {});

std::shared_ptr<arrow::Schema> DeserializeSchema(const TString& str);

TString SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options);
TString SerializeBatchNoCompression(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::RecordBatch> DeserializeBatch(const TString& blob,
                                                     const std::shared_ptr<arrow::Schema>& schema);

std::shared_ptr<arrow::RecordBatch> SortBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique);
std::shared_ptr<arrow::RecordBatch> SortBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::shared_ptr<arrow::Array>>& sortingKey, const bool andUnique);
bool IsSorted(const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& sortingKey,
    bool desc = false);
bool IsSortedAndUnique(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey,
                       bool desc = false);
void DedupSortedBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey,
                       std::vector<std::shared_ptr<arrow::RecordBatch>>& out);

[[nodiscard]] std::shared_ptr<arrow::RecordBatch> ReallocateBatch(std::shared_ptr<arrow::RecordBatch> original);
[[nodiscard]] std::shared_ptr<arrow::Table> ReallocateBatch(
    const std::shared_ptr<arrow::Table>& original, arrow::MemoryPool* pool = arrow::default_memory_pool());
[[nodiscard]] std::shared_ptr<arrow::ChunkedArray> ReallocateArray(
    const std::shared_ptr<arrow::ChunkedArray>& original, arrow::MemoryPool* pool = arrow::default_memory_pool());
[[nodiscard]] std::shared_ptr<arrow::Array> ReallocateArray(const std::shared_ptr<arrow::Array>& arr, arrow::MemoryPool* pool = arrow::default_memory_pool());

}
