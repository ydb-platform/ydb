#pragma once
#include "switch_type.h"
#include "size_calcer.h"
#include <ydb/core/formats/factory.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;
template<typename T>
class TReplaceKeyTemplate;
using TReplaceKey = TReplaceKeyTemplate<std::shared_ptr<TArrayVec>>;

// Arrow inrernally keeps references to Buffer objects with the data
// This helper class implements arrow::Buffer over TString that owns
// the actual memory
class TBufferOverString : public arrow::Buffer {
    TString Str;
public:
    explicit TBufferOverString(TString str)
        : arrow::Buffer((const unsigned char*)str.data(), str.size())
        , Str(str)
    {
        Y_VERIFY(data() == (const unsigned char*)Str.data());
    }
};

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

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns);
std::shared_ptr<arrow::Schema> MakeArrowSchema(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns);

TString SerializeSchema(const arrow::Schema& schema);
std::shared_ptr<arrow::Schema> DeserializeSchema(const TString& str);

TString SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options);
TString SerializeBatchNoCompression(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::RecordBatch> DeserializeBatch(const TString& blob,
                                                     const std::shared_ptr<arrow::Schema>& schema);
std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow::Schema>& schema);

std::shared_ptr<arrow::RecordBatch> ExtractColumns(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
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
std::vector<std::shared_ptr<arrow::RecordBatch>> MergeSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows);
std::vector<std::shared_ptr<arrow::RecordBatch>> SliceSortedBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                                    const std::shared_ptr<TSortDescription>& description,
                                                                    size_t maxBatchRows = 0);
std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<ui32>& sharding,
    ui32 numShards);

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(const std::shared_ptr<arrow::Schema>& schema,
    size_t reserve = 0, const std::map<std::string, ui64>& sizeByColumn = {});
std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders);

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(ui64 value, i64 size);
std::shared_ptr<arrow::UInt64Array> MakePermutation(int size, bool reverse = false);
std::shared_ptr<arrow::BooleanArray> MakeFilter(const std::vector<bool>& bits);
std::vector<bool> CombineFilters(std::vector<bool>&& f1, std::vector<bool>&& f2);
std::vector<bool> CombineFilters(std::vector<bool>&& f1, std::vector<bool>&& f2, size_t& count);
TVector<TString> ColumnNames(const std::shared_ptr<arrow::Schema>& schema);
i64 LowerBound(const std::shared_ptr<arrow::Array>& column, const arrow::Scalar& value, i64 offset = 0);
i64 LowerBound(const std::shared_ptr<arrow::RecordBatch>& batch, const TReplaceKey& key, i64 offset = 0);
bool ReserveData(arrow::ArrayBuilder& builder, const size_t size);
enum class ECompareType {
    LESS = 1,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
};

// It makes a filter using composite predicate. You need MakeFilter() + arrow::Filter() to apply it to Datum.
std::vector<bool> MakePredicateFilter(const arrow::Datum& datum, const arrow::Datum& border, ECompareType compareType);
std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const std::shared_ptr<arrow::Schema>& sortingKey);
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
bool ScalarLess(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y);
bool ScalarLess(const arrow::Scalar& x, const arrow::Scalar& y);

inline bool HasNulls(const std::shared_ptr<arrow::Array>& column) {
    return column->null_bitmap_data();
}

bool ArrayScalarsEqual(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs);
std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec);

}
