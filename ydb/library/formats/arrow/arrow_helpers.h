#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <library/cpp/json/writer/json_value.h>

#include <map>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow20::Array>>;

template <typename T>
inline bool ArrayEqualValue(const std::shared_ptr<arrow20::Array>& x, const std::shared_ptr<arrow20::Array>& y) {
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
inline bool ArrayEqualView(const std::shared_ptr<arrow20::Array>& x, const std::shared_ptr<arrow20::Array>& y) {
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

TString SerializeSchema(const arrow20::Schema& schema);

std::shared_ptr<arrow20::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow20::Schema>& schema, const ui32 rowsCount = 0);
std::shared_ptr<arrow20::Table> ToTable(const std::shared_ptr<arrow20::RecordBatch>& batch);

std::shared_ptr<arrow20::RecordBatch> ToBatch(const std::shared_ptr<arrow20::Table>& combinedTable);
std::shared_ptr<arrow20::RecordBatch> CombineBatches(const std::vector<std::shared_ptr<arrow20::RecordBatch>>& batches);
std::shared_ptr<arrow20::RecordBatch> MergeColumns(const std::vector<std::shared_ptr<arrow20::RecordBatch>>& rb);
std::vector<std::shared_ptr<arrow20::RecordBatch>> ShardingSplit(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const std::vector<ui32>& sharding, ui32 numShards);
std::vector<std::shared_ptr<arrow20::RecordBatch>> ShardingSplit(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const std::vector<std::vector<ui32>>& shardRows, const ui32 numShards);
THashMap<ui64, std::shared_ptr<arrow20::RecordBatch>> ShardingSplit(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const THashMap<ui64, std::vector<ui32>>& shardRows, arrow20::MemoryPool* memoryPool);

std::unique_ptr<arrow20::ArrayBuilder> MakeBuilder(
    const std::shared_ptr<arrow20::Field>& field, const ui32 reserveItems = 0, const ui32 reserveSize = 0);
std::unique_ptr<arrow20::ArrayBuilder> MakeBuilder(
    const std::shared_ptr<arrow20::DataType>& type, const ui32 reserveItems = 0, const ui32 reserveSize = 0);

std::vector<std::unique_ptr<arrow20::ArrayBuilder>> MakeBuilders(
    const std::shared_ptr<arrow20::Schema>& schema, size_t reserve = 0, const std::map<std::string, ui64>& sizeByColumn = {});
std::vector<std::shared_ptr<arrow20::Array>> Finish(std::vector<std::unique_ptr<arrow20::ArrayBuilder>>&& builders);
std::vector<std::shared_ptr<arrow20::Array>> FinishBuilders(std::vector<std::unique_ptr<arrow20::ArrayBuilder>>&& builders);
std::shared_ptr<arrow20::Array> FinishBuilder(std::unique_ptr<arrow20::ArrayBuilder>&& builders);

std::shared_ptr<arrow20::UInt64Array> MakeUI64Array(const ui64 value, const i64 size);
std::shared_ptr<arrow20::StringArray> MakeStringArray(const TString& value, const i64 size);
std::vector<TString> ColumnNames(const std::shared_ptr<arrow20::Schema>& schema);
bool ReserveData(arrow20::ArrayBuilder& builder, const size_t size);
bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow20::RecordBatch>>& batches, std::shared_ptr<arrow20::RecordBatch>& result,
    const std::vector<std::string>& columnsOrder = {}, const bool orderFieldsAreNecessary = true);
bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow20::Table>>& batches, std::shared_ptr<arrow20::Table>& result,
    const std::vector<std::string>& columnsOrder = {}, const bool orderFieldsAreNecessary = true);

bool HasAllColumns(const std::shared_ptr<arrow20::RecordBatch>& batch, const std::shared_ptr<arrow20::Schema>& schema);

std::pair<int, int> FindMinMaxPosition(const std::shared_ptr<arrow20::Array>& column);

std::shared_ptr<arrow20::Scalar> DefaultScalar(const std::shared_ptr<arrow20::DataType>& type);
std::shared_ptr<arrow20::Scalar> MinScalar(const std::shared_ptr<arrow20::DataType>& type);
std::shared_ptr<arrow20::Scalar> GetScalar(const std::shared_ptr<arrow20::Array>& array, int position);
bool IsGoodScalar(const std::shared_ptr<arrow20::Scalar>& x);
TConclusion<bool> ScalarIsFalse(const arrow20::Scalar& x);
TConclusion<bool> ScalarIsTrue(const arrow20::Scalar& x);
TConclusion<bool> ScalarIsFalse(const std::shared_ptr<arrow20::Scalar>& x);
TConclusion<bool> ScalarIsTrue(const std::shared_ptr<arrow20::Scalar>& x);
int ScalarCompare(const arrow20::Scalar& x, const arrow20::Scalar& y);
int ScalarCompare(const std::shared_ptr<arrow20::Scalar>& x, const std::shared_ptr<arrow20::Scalar>& y);
int ScalarCompareNullable(const std::shared_ptr<arrow20::Scalar>& x, const std::shared_ptr<arrow20::Scalar>& y);
std::partial_ordering ColumnsCompare(
    const std::vector<std::shared_ptr<arrow20::Array>>& x, const ui32 xRow, const std::vector<std::shared_ptr<arrow20::Array>>& y, const ui32 yRow);
bool ColumnEqualsScalar(const std::shared_ptr<arrow20::Array>& c, const ui32 position, const std::shared_ptr<arrow20::Scalar>& s);
bool ScalarLess(const std::shared_ptr<arrow20::Scalar>& x, const std::shared_ptr<arrow20::Scalar>& y);
bool ScalarLess(const arrow20::Scalar& x, const arrow20::Scalar& y);

bool HasNulls(const std::shared_ptr<arrow20::Array>& column);

std::vector<std::shared_ptr<arrow20::RecordBatch>> SliceToRecordBatches(const std::shared_ptr<arrow20::Table>& t);

bool ArrayScalarsEqual(const std::shared_ptr<arrow20::Array>& lhs, const std::shared_ptr<arrow20::Array>& rhs);
std::shared_ptr<arrow20::Array> BoolVecToArray(const std::vector<bool>& vec);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow20::Array> array, const ui32 head, const ui32 tail);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow20::RecordBatch> batch, const ui32 head, const ui32 tail);

NJson::TJsonValue DebugJson(std::shared_ptr<arrow20::Array> array, const ui32 position);
TString DebugString(std::shared_ptr<arrow20::Array> array, const ui32 position);
NJson::TJsonValue DebugJson(std::shared_ptr<arrow20::RecordBatch> array, const ui32 position);

std::shared_ptr<arrow20::RecordBatch> Reorder(
    const std::shared_ptr<arrow20::RecordBatch>& batch,
    const std::shared_ptr<arrow20::UInt64Array>& permutation,
    const bool canRemove,
    arrow20::MemoryPool* pool = arrow20::default_memory_pool());

// Deep-copies all internal arrow20::buffers - and makes sure that new buffers don't have any parents.
std::shared_ptr<arrow20::Table> DeepCopy(const std::shared_ptr<arrow20::Table>& table, arrow20::MemoryPool* pool = arrow20::default_memory_pool());

}   // namespace NKikimr::NArrow
