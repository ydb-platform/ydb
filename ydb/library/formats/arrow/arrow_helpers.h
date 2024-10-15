#pragma once
#include "switch_type.h"
#include <library/cpp/json/writer/json_value.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <ydb/library/accessor/accessor.h>
#include <map>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

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

TString SerializeSchema(const arrow::Schema& schema);

std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow::Schema>& schema, const ui32 rowsCount = 0);
std::shared_ptr<arrow::Table> ToTable(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table>& combinedTable, const bool combine);
std::shared_ptr<arrow::RecordBatch> CombineBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
std::shared_ptr<arrow::RecordBatch> MergeColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& rb);
std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<ui32>& sharding, ui32 numShards);
std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::vector<ui32>>& shardRows, const ui32 numShards);
THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch, const THashMap<ui64, std::vector<ui32>>& shardRows);

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field>& field);
std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::DataType>& type);

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(const std::shared_ptr<arrow::Schema>& schema,
    size_t reserve = 0, const std::map<std::string, ui64>& sizeByColumn = {});
std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders);

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(ui64 value, i64 size);
std::vector<TString> ColumnNames(const std::shared_ptr<arrow::Schema>& schema);
bool ReserveData(arrow::ArrayBuilder& builder, const size_t size);
bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::shared_ptr<arrow::RecordBatch>& result, const std::vector<std::string>& columnsOrder = {}, const bool orderFieldsAreNecessary = true);
bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::Table>>& batches, std::shared_ptr<arrow::Table>& result, const std::vector<std::string>& columnsOrder = {}, const bool orderFieldsAreNecessary = true);

bool HasAllColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& schema);

std::pair<int, int> FindMinMaxPosition(const std::shared_ptr<arrow::Array>& column);

std::shared_ptr<arrow::Scalar> DefaultScalar(const std::shared_ptr<arrow::DataType>& type);
std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type);
std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array, int position);
bool IsGoodScalar(const std::shared_ptr<arrow::Scalar>& x);
int ScalarCompare(const arrow::Scalar& x, const arrow::Scalar& y);
int ScalarCompare(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
int ScalarCompareNullable(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
std::partial_ordering ColumnsCompare(
    const std::vector<std::shared_ptr<arrow::Array>>& x, const ui32 xRow, const std::vector<std::shared_ptr<arrow::Array>>& y, const ui32 yRow);
bool ColumnEqualsScalar(
    const std::shared_ptr<arrow::Array>& c, const ui32 position, const std::shared_ptr<arrow::Scalar>& s);
bool ScalarLess(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
bool ScalarLess(const arrow::Scalar& x, const arrow::Scalar& y);

bool HasNulls(const std::shared_ptr<arrow::Array>& column);

std::vector<std::shared_ptr<arrow::RecordBatch>> SliceToRecordBatches(const std::shared_ptr<arrow::Table>& t);

bool ArrayScalarsEqual(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs);
std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 head, const ui32 tail);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> batch, const ui32 head, const ui32 tail);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 position);
TString DebugString(std::shared_ptr<arrow::Array> array, const ui32 position);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> array, const ui32 position);

std::shared_ptr<arrow::RecordBatch> Reorder(const std::shared_ptr<arrow::RecordBatch>& batch,
                                            const std::shared_ptr<arrow::UInt64Array>& permutation, const bool canRemove);

}
