#include "arrow_helpers.h"
#include "permutations.h"
#include "key_comparator.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/services/services.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

#include <numeric>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::vector<std::shared_ptr<arrow::Array>>& keyColumns, const bool andUnique,
    const arrow::Schema* comparisonSchema, const bool preserveEqualOrder) {
    AFL_VERIFY(!preserveEqualOrder || !andUnique);
    std::optional<i64> count;
    for (auto&& i : keyColumns) {
        AFL_VERIFY(i);
        if (!count) {
            count = i->length();
        } else {
            AFL_VERIFY(*count == i->length());
        }
    }
    AFL_VERIFY(count);
    AFL_VERIFY(*count <= Max<i32>());
    std::vector<i32> positions(*count);
    std::iota(positions.begin(), positions.end(), 0);

    const TKeyComparator comparator(comparisonSchema, keyColumns.size());
    std::sort(positions.begin(), positions.end(), [&](const i32 a, const i32 b) {
        const auto cmp = comparator.Compare(keyColumns, a, b);
        return cmp == std::partial_ordering::equivalent
            ? (preserveEqualOrder ? a < b : a > b)
            : cmp == std::partial_ordering::less;
    });

    arrow::UInt64Builder builder;
    TStatusValidator::Validate(builder.Reserve(positions.size()));

    i32 previous = -1;
    bool isTrivial = true;
    for (const i32 position : positions) {
        if (andUnique && previous != -1 &&
            comparator.Compare(keyColumns, previous, position) == std::partial_ordering::equivalent) {
            continue;
        }
        if (position != previous + 1) {
            isTrivial = false;
        }
        previous = position;
        TStatusValidator::Validate(builder.Append(position));
    }

    if (isTrivial && builder.length() == *count) {
        return nullptr;
    }

    std::shared_ptr<arrow::UInt64Array> out;
    TStatusValidator::Validate(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique,
    const bool preserveEqualOrder) {
    auto keyBatch = TColumnOperator().VerifyIfAbsent().Adapt(batch, sortingKey).DetachResult();
    return MakeSortPermutation(keyBatch->columns(), andUnique, sortingKey.get(), preserveEqualOrder);
}

}   // namespace NKikimr::NArrow
