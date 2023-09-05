#include "arrow_helpers.h"
#include "permutations.h"
#include "replace_key.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::UInt64Array> MakePermutation(const int size, const bool reverse) {
    if (size < 1) {
        return {};
    }

    arrow::UInt64Builder builder;
    if (!builder.Reserve(size).ok()) {
        return {};
    }

    if (reverse) {
        ui64 value = size - 1;
        for (i64 i = 0; i < size; ++i, --value) {
            if (!builder.Append(value).ok()) {
                return {};
            }
        }
    } else {
        for (i64 i = 0; i < size; ++i) {
            if (!builder.Append(i).ok()) {
                return {};
            }
        }
    }

    std::shared_ptr<arrow::UInt64Array> out;
    if (!builder.Finish(&out).ok()) {
        return {};
    }
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const std::shared_ptr<arrow::Schema>& sortingKey) {
    auto keyBatch = ExtractColumns(batch, sortingKey);
    auto keyColumns = std::make_shared<TArrayVec>(keyBatch->columns());
    std::vector<TRawReplaceKey> points;
    points.reserve(keyBatch->num_rows());

    for (int i = 0; i < keyBatch->num_rows(); ++i) {
        points.push_back(TRawReplaceKey(keyColumns.get(), i));
    }

    bool haveNulls = false;
    for (auto& column : *keyColumns) {
        if (HasNulls(column)) {
            haveNulls = true;
            break;
        }
    }

    if (haveNulls) {
        std::sort(points.begin(), points.end());
    } else {
        std::sort(points.begin(), points.end(),
            [](const TRawReplaceKey& a, const TRawReplaceKey& b) {
                return a.CompareNotNull(b) == std::partial_ordering::less;
            }
        );
    }

    arrow::UInt64Builder builder;
    TStatusValidator::Validate(builder.Reserve(points.size()));

    for (auto& point : points) {
        TStatusValidator::Validate(builder.Append(point.GetPosition()));
    }

    std::shared_ptr<arrow::UInt64Array> out;
    TStatusValidator::Validate(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui64>& indexes) {
    if (indexes.empty()) {
        return {};
    }

    arrow::UInt64Builder builder;
    if (!builder.Reserve(indexes.size()).ok()) {
        return {};
    }

    for (auto&& i : indexes) {
        TStatusValidator::Validate(builder.Append(i));
    }
    std::shared_ptr<arrow::UInt64Array> out;
    TStatusValidator::Validate(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::Array> CopyRecords(const std::shared_ptr<arrow::Array>& source, const std::vector<ui64>& indexes) {
    if (!source) {
        return source;
    }
    std::shared_ptr<arrow::Array> result;
    SwitchType(source->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;
        auto& column = static_cast<const TArray&>(*source);

        std::unique_ptr<arrow::ArrayBuilder> builder;
        TStatusValidator::Validate(arrow::MakeBuilder(arrow::default_memory_pool(), source->type(), &builder));
        TStatusValidator::Validate(builder->Reserve(indexes.size()));

        {
            auto& builderImpl = static_cast<TBuilder&>(*builder);
            for (auto&& i : indexes) {
                TStatusValidator::Validate(builderImpl.Append(column.GetView(i)));
            }
        }

        TStatusValidator::Validate(builder->Finish(&result));
        return true;
    });
    Y_VERIFY(result);
    return result;
}

}
