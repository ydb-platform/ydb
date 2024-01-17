#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

class THashConstructor {
public:
    static bool BuildHashUI64(std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName);
};

class TShardedRecordBatch {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, RecordBatch);
    YDB_READONLY_DEF(std::vector<std::vector<ui32>>, SplittedByShards);
public:
    TShardedRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch);

    void Cut(const ui32 limit) {
        RecordBatch = RecordBatch->Slice(0, limit);
        for (auto&& i : SplittedByShards) {
            auto it = std::lower_bound(i.begin(), i.end(), limit);
            if (it != i.end()) {
                i.erase(it, i.end());
            }
        }
    }

    bool IsSharded() const {
        return SplittedByShards.size() > 1;
    }

    TShardedRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch, std::vector<std::vector<ui32>>&& splittedByShards);

    void StripColumns(const std::shared_ptr<arrow::Schema>& schema) {
        RecordBatch = NArrow::ExtractColumns(RecordBatch, schema);
    }

    ui64 GetMemorySize() const;

    ui64 GetRecordsCount() const {
        return RecordBatch->num_rows();
    }
};

class TShardingSplitIndex {
private:
    ui32 ShardsCount = 0;
    std::vector<std::vector<ui32>> Remapping;
    ui32 RecordsCount = 0;

    template <class TIterator>
    std::vector<ui32> MergeLists(const std::vector<ui32>& base, const TIterator itFrom, const TIterator itTo) {
        std::vector<ui32> result;
        result.reserve(base.size() + (itTo - itFrom));
        auto itBase = base.begin();
        auto itExt = itFrom;
        while (itBase != base.end() && itExt != itTo) {
            if (*itBase < *itExt) {
                result.emplace_back(*itBase);
                ++itBase;
            } else {
                result.emplace_back(*itExt);
                ++itExt;
            }
        }
        if (itBase == base.end()) {
            result.insert(result.end(), itExt, itTo);
        } else if (itExt == itTo) {
            result.insert(result.end(), itBase, base.end());
        }
        return result;
    }

    template <class TIntArrowArray>
    void Initialize(const TIntArrowArray& arrowHashArray) {
        Y_ABORT_UNLESS(ShardsCount);
        Remapping.resize(ShardsCount);
        const ui32 expectation = arrowHashArray.length() / ShardsCount + 1;
        for (auto&& i : Remapping) {
            i.reserve(2 * expectation);
        }
        for (ui64 i = 0; i < (ui64)arrowHashArray.length(); ++i) {
            const i64 v = arrowHashArray.GetView(i);
            const ui32 idx = ((v < 0) ? (-v) : v) % ShardsCount;
            Remapping[idx].emplace_back(i);
        }
        std::deque<std::vector<ui32>*> sizeCorrection;
        for (auto&& i : Remapping) {
            sizeCorrection.emplace_back(&i);
        }
        const auto pred = [](const std::vector<ui32>* l, const std::vector<ui32>* r) {
            return l->size() < r->size();
        };
        std::sort(sizeCorrection.begin(), sizeCorrection.end(), pred);
        while (sizeCorrection.size() > 1 && sizeCorrection.back()->size() > expectation && sizeCorrection.front()->size() < expectation) {
            const ui32 uselessRecords = sizeCorrection.back()->size() - expectation;
            const ui32 needRecords = expectation - sizeCorrection.front()->size();
            const ui32 moveRecords = std::min<ui32>(needRecords, uselessRecords);
            if (moveRecords == 0) {
                break;
            }
            *sizeCorrection.front() = MergeLists(*sizeCorrection.front(), sizeCorrection.back()->end() - moveRecords, sizeCorrection.back()->end());
            sizeCorrection.back()->resize(sizeCorrection.back()->size() - moveRecords);
            if (sizeCorrection.back()->size() <= expectation) {
                sizeCorrection.pop_back();
            }
            if (sizeCorrection.front()->size() >= expectation) {
                sizeCorrection.pop_front();
            }
        }
    }

    TShardingSplitIndex(const ui32 shardsCount, const arrow::Array& arrowHashArray)
        : ShardsCount(shardsCount)
        , RecordsCount(arrowHashArray.length()) {
    }

public:

    std::vector<std::vector<ui32>> DetachRemapping() {
        return std::move(Remapping);
    }

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
std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui32>& indexes);
std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique);
std::shared_ptr<arrow::RecordBatch> ReverseRecords(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::Array> CopyRecords(const std::shared_ptr<arrow::Array>& source, const std::vector<ui64>& indexes);
std::shared_ptr<arrow::RecordBatch> CopyRecords(const std::shared_ptr<arrow::RecordBatch>& source, const std::vector<ui64>& indexes);

}
