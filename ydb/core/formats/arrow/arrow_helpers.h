#pragma once
#include "switch_type.h"
#include <ydb/core/formats/factory.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <library/cpp/json/writer/json_value.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

std::shared_ptr<arrow::DataType> GetArrowType(NScheme::TTypeInfo typeInfo);
std::shared_ptr<arrow::DataType> GetCSVArrowType(NScheme::TTypeInfo typeId);

template <typename T>
inline bool ArrayEqualValue(const std::shared_ptr<arrow::Array>& x, const std::shared_ptr<arrow::Array>& y) {
    auto& arrX = static_cast<const T&>(*x);
    auto& arrY = static_cast<const T&>(*y);
    for (int i = 0; i < x->length(); ++i) {
        if (arrX.Value(i) != arrY.Value(i)) {
            return false;
        }
    }
    return true;
}

template <typename T>
inline bool ArrayEqualView(const std::shared_ptr<arrow::Array>& x, const std::shared_ptr<arrow::Array>& y) {
    auto& arrX = static_cast<const T&>(*x);
    auto& arrY = static_cast<const T&>(*y);
    for (int i = 0; i < x->length(); ++i) {
        if (arrX.GetView(i) != arrY.GetView(i)) {
            return false;
        }
    }
    return true;
}

struct TSortDescription;

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);
std::shared_ptr<arrow::Schema> MakeArrowSchema(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);

TString SerializeSchema(const arrow::Schema& schema);
std::shared_ptr<arrow::Schema> DeserializeSchema(const TString& str);

TString SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options);
TString SerializeBatchNoCompression(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::RecordBatch> DeserializeBatch(const TString& blob,
                                                     const std::shared_ptr<arrow::Schema>& schema);
std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow::Schema>& schema, const ui32 rowsCount = 0);

std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::vector<TString>& columnNames);
std::shared_ptr<arrow::RecordBatch> ExtractColumnsValidate(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::vector<TString>& columnNames);
std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                   const std::shared_ptr<arrow::Schema>& dstSchema,
                                                   bool addNotExisted = false);
std::shared_ptr<arrow::RecordBatch> ExtractExistedColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                          const arrow::FieldVector& fields);
inline std::shared_ptr<arrow::RecordBatch> ExtractExistedColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                                 const std::shared_ptr<arrow::Schema>& dstSchema)
{
    return ExtractExistedColumns(srcBatch, dstSchema->fields());
}

std::shared_ptr<arrow::Table> CombineInTable(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table>& combinedTable);
std::shared_ptr<arrow::RecordBatch> CombineBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
std::shared_ptr<arrow::RecordBatch> CombineSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                         const std::shared_ptr<TSortDescription>& description);
std::shared_ptr<arrow::RecordBatch> MergeColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& rb);
std::vector<std::shared_ptr<arrow::RecordBatch>> MergeSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows);
std::vector<std::shared_ptr<arrow::RecordBatch>> SliceSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows = 0);
std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<ui32>& sharding,
    ui32 numShards);

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field>& field);

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(const std::shared_ptr<arrow::Schema>& schema,
    size_t reserve = 0, const std::map<std::string, ui64>& sizeByColumn = {});
std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders);

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(ui64 value, i64 size);
std::vector<TString> ColumnNames(const std::shared_ptr<arrow::Schema>& schema);
bool ReserveData(arrow::ArrayBuilder& builder, const size_t size);
bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::shared_ptr<arrow::RecordBatch>& result, const std::vector<std::string>& columnsOrder = {}, const bool orderFieldsAreNecessary = true);

std::shared_ptr<arrow::RecordBatch> SortBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                              const std::shared_ptr<arrow::Schema>& sortingKey);
bool IsSorted(const std::shared_ptr<arrow::RecordBatch>& batch,
              const std::shared_ptr<arrow::Schema>& sortingKey,
              bool desc = false);
bool IsSortedAndUnique(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey,
                       bool desc = false);
void DedupSortedBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& sortingKey,
                       std::vector<std::shared_ptr<arrow::RecordBatch>>& out);
bool HasAllColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& schema);

std::pair<int, int> FindMinMaxPosition(const std::shared_ptr<arrow::Array>& column);
std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type);
std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array, int position);
bool IsGoodScalar(const std::shared_ptr<arrow::Scalar>& x);
int ScalarCompare(const arrow::Scalar& x, const arrow::Scalar& y);
int ScalarCompare(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
std::partial_ordering ColumnsCompare(const std::vector<std::shared_ptr<arrow::Array>>& x, const ui32 xRow, const std::vector<std::shared_ptr<arrow::Array>>& y, const ui32 yRow);
bool ScalarLess(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
bool ScalarLess(const arrow::Scalar& x, const arrow::Scalar& y);
std::shared_ptr<arrow::RecordBatch> ReallocateBatch(std::shared_ptr<arrow::RecordBatch> original);

std::shared_ptr<arrow::RecordBatch> BuildSingleRecordBatch(const std::shared_ptr<arrow::Schema> schema, const std::vector<std::shared_ptr<arrow::Scalar>>& recordData);

inline bool HasNulls(const std::shared_ptr<arrow::Array>& column) {
    return column->null_bitmap_data();
}

bool ArrayScalarsEqual(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs);
std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 head, const ui32 tail);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> batch, const ui32 head, const ui32 tail);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 position);
TString DebugString(std::shared_ptr<arrow::Array> array, const ui32 position);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> array, const ui32 position);

}
