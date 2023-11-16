#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

class THashConstructor {
public:
    static bool BuildHashUI64(std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName);

    static size_t TypedHash(const std::vector<std::shared_ptr<arrow::Array>>& ar, const int pos);
    static size_t TypedHash(const arrow::Array& ar, const int pos);
};

class TShardedRecordBatch {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, RecordBatch);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::RecordBatch>>, SplittedByShards);
public:
    TShardedRecordBatch() = default;
    TShardedRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch)
        : RecordBatch(batch) {
        SplittedByShards = {RecordBatch};
    }

    TShardedRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch, std::vector<std::shared_ptr<arrow::RecordBatch>>&& splittedByShards)
        : RecordBatch(batch)
        , SplittedByShards(std::move(splittedByShards))
    {
    }

    ui64 GetMemorySize() const;

    ui64 GetRecordsCount() const {
        return RecordBatch->num_rows();
    }
};

class TShardingSplitIndex {
private:
    ui32 ShardsCount = 0;
    std::vector<std::vector<ui64>> Remapping;
    ui32 RecordsCount = 0;

    template <class TIntArrowArray>
    void Initialize(const TIntArrowArray& arrowHashArray) {
        Y_ABORT_UNLESS(ShardsCount);
        Remapping.resize(ShardsCount);
        for (ui64 i = 0; i < (ui64)arrowHashArray.length(); ++i) {
            const auto v = arrowHashArray.GetView(i);
            if (v < 0) {
                Remapping[(-v) % ShardsCount].emplace_back(i);
            } else {
                Remapping[v % ShardsCount].emplace_back(i);
            }
        }
    }

    TShardingSplitIndex(const ui32 shardsCount, const arrow::Array& arrowHashArray)
        : ShardsCount(shardsCount)
        , RecordsCount(arrowHashArray.length()) {
    }

public:

    template <class TArrayClass>
    static TShardingSplitIndex Build(const ui32 shardsCount, const arrow::Array& arrowHashArray) {
        TShardingSplitIndex result(shardsCount, arrowHashArray);
        result.Initialize<TArrayClass>(static_cast<const TArrayClass&>(arrowHashArray));
        return result;
    }

    std::shared_ptr<arrow::UInt64Array> BuildPermutation() const;

    std::vector<std::shared_ptr<arrow::RecordBatch>> Apply(const std::shared_ptr<arrow::RecordBatch>& input);

    static TShardedRecordBatch Apply(const ui32 shardsCount, const std::shared_ptr<arrow::RecordBatch>& input, const std::string& hashColumnName);
};

std::shared_ptr<arrow::UInt64Array> MakePermutation(const int size, const bool reverse = false);
std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui64>& indexes);
std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique);

std::shared_ptr<arrow::Array> CopyRecords(const std::shared_ptr<arrow::Array>& source, const std::vector<ui64>& indexes);
std::shared_ptr<arrow::RecordBatch> CopyRecords(const std::shared_ptr<arrow::RecordBatch>& source, const std::vector<ui64>& indexes);

}
