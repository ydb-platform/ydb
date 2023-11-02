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
                                                        const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique) {
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

std::shared_ptr<arrow::RecordBatch> CopyRecords(const std::shared_ptr<arrow::RecordBatch>& source, const std::vector<ui64>& indexes) {
    Y_ABORT_UNLESS(!!source);
    auto schema = source->schema();
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto&& i : source->columns()) {
        columns.emplace_back(CopyRecords(i, indexes));
    }
    return arrow::RecordBatch::Make(schema, indexes.size(), columns);
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
        auto& builderImpl = static_cast<TBuilder&>(*builder);

        if constexpr (arrow::has_string_view<typename TWrap::T>::value) {
            ui64 sumByIndexes = 0;
            for (auto&& idx : indexes) {
                sumByIndexes += column.GetView(idx).size();
            }
            TStatusValidator::Validate(builderImpl.ReserveData(sumByIndexes));
        }

        TStatusValidator::Validate(builder->Reserve(indexes.size()));

        {
            const ui32 arraySize = column.length();
            for (auto&& i : indexes) {
                Y_ABORT_UNLESS(i < arraySize);
                builderImpl.UnsafeAppend(column.GetView(i));
            }
        }

        TStatusValidator::Validate(builder->Finish(&result));
        return true;
    });
    Y_ABORT_UNLESS(result);
    return result;
}

}
