#include "arrow_helpers.h"
#include "permutations.h"
#include "key_comparator.h"

#include "hash/calcer.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/services/services.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

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
    std::vector<TRawReplaceKey> points;
    points.reserve(*count);
    for (int i = 0; i < *count; ++i) {
        points.push_back(TRawReplaceKey(&keyColumns, i));
    }

    bool haveNulls = false;
    for (auto& column : keyColumns) {
        if (HasNulls(column)) {
            haveNulls = true;
            break;
        }
    }

    const TKeyComparator comparator(comparisonSchema, keyColumns.size());
    std::sort(points.begin(), points.end(), [&](const TRawReplaceKey& a, const TRawReplaceKey& b) {
        const auto cmp = comparator.Compare(keyColumns, a.GetPosition(), b.GetPosition());
        return cmp == std::partial_ordering::equivalent
            ? (preserveEqualOrder ? a.GetPosition() < b.GetPosition() : a.GetPosition() > b.GetPosition())
            : cmp == std::partial_ordering::less;
    });

    arrow::UInt64Builder builder;
    TStatusValidator::Validate(builder.Reserve(points.size()));

    TRawReplaceKey* predKey = nullptr;
    int predPosition = -1;
    bool isTrivial = true;
    for (auto& point : points) {
        if (andUnique) {
            if (predKey) {
                if (haveNulls) {
                    if (*predKey == point) {
                        continue;
                    }
                } else if (predKey->CompareNotNull(point) == std::partial_ordering::equivalent) {
                    continue;
                }
            }
        }
        if (point.GetPosition() != predPosition + 1) {
            isTrivial = false;
        }
        predPosition = point.GetPosition();
        TStatusValidator::Validate(builder.Append(point.GetPosition()));
        predKey = &point;
    }

    if (isTrivial && builder.length() == (i64)points.size()) {
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
