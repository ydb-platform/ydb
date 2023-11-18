#include "arrow_helpers.h"
#include "permutations.h"
#include "replace_key.h"
#include "size_calcer.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/actors/core/log.h>

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
                Y_ABORT_UNLESS(idx < (ui64)column.length());
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

bool THashConstructor::BuildHashUI64(std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    if (fieldNames.size() == 0) {
        return false;
    }
    Y_ABORT_UNLESS(!batch->GetColumnByName(hashFieldName));
    if (fieldNames.size() == 1) {
        auto column = batch->GetColumnByName(fieldNames.front());
        if (!column) {
            return false;
        }
        Y_ABORT_UNLESS(column);
        if (column->type()->id() == arrow::Type::UINT64 || column->type()->id() == arrow::Type::UINT32 || column->type()->id() == arrow::Type::INT64 || column->type()->id() == arrow::Type::INT32) {
            batch = TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), hashFieldName, column));
            return true;
        }
    }
    auto builder = NArrow::MakeBuilder(std::make_shared<arrow::Field>(hashFieldName, arrow::TypeTraits<arrow::UInt64Type>::type_singleton()));
    {
        auto& intBuilder = static_cast<arrow::UInt64Builder&>(*builder);
        TStatusValidator::Validate(intBuilder.Reserve(batch->num_rows()));
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (auto&& i : fieldNames) {
            auto column = batch->GetColumnByName(i);
//            AFL_VERIFY(column)("column_name", i)("all_columns", JoinSeq(",", fieldNames));
            if (column) {
                columns.emplace_back(column);
            }
        }
        if (columns.empty()) {
            return false;
        }
        for (i64 i = 0; i < batch->num_rows(); ++i) {
            intBuilder.UnsafeAppend(TypedHash(columns, i));
        }
    }
    batch = TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), hashFieldName, NArrow::TStatusValidator::GetValid(builder->Finish())));
    return true;
}

size_t THashConstructor::TypedHash(const std::vector<std::shared_ptr<arrow::Array>>& ar, const int pos) {
    size_t result = 0;
    for (auto&& i : ar) {
        result = CombineHashes(result, TypedHash(*i, pos));
    }
    return result;
}

size_t THashConstructor::TypedHash(const arrow::Array& ar, const int pos) {
    switch (ar.type_id()) {
        case arrow::Type::BOOL:
            return (size_t)(static_cast<const arrow::BooleanArray&>(ar).Value(pos));
        case arrow::Type::UINT8:
            return THash<ui8>()(static_cast<const arrow::UInt8Array&>(ar).Value(pos));
        case arrow::Type::INT8:
            return THash<i8>()(static_cast<const arrow::Int8Array&>(ar).Value(pos));
        case arrow::Type::UINT16:
            return THash<ui16>()(static_cast<const arrow::UInt16Array&>(ar).Value(pos));
        case arrow::Type::INT16:
            return THash<i16>()(static_cast<const arrow::Int16Array&>(ar).Value(pos));
        case arrow::Type::UINT32:
            return THash<ui32>()(static_cast<const arrow::UInt32Array&>(ar).Value(pos));
        case arrow::Type::INT32:
            return THash<i32>()(static_cast<const arrow::Int32Array&>(ar).Value(pos));
        case arrow::Type::UINT64:
            return THash<ui64>()(static_cast<const arrow::UInt64Array&>(ar).Value(pos));
        case arrow::Type::INT64:
            return THash<i64>()(static_cast<const arrow::Int64Array&>(ar).Value(pos));
        case arrow::Type::HALF_FLOAT:
            break;
        case arrow::Type::FLOAT:
            return THash<float>()(static_cast<const arrow::FloatArray&>(ar).Value(pos));
        case arrow::Type::DOUBLE:
            return THash<double>()(static_cast<const arrow::DoubleArray&>(ar).Value(pos));
        case arrow::Type::STRING:
        {
            const auto& str = static_cast<const arrow::StringArray&>(ar).GetView(pos);
            return CityHash64(str.data(), str.size());
        }
        case arrow::Type::BINARY:
        {
            const auto& str = static_cast<const arrow::BinaryArray&>(ar).GetView(pos);
            return CityHash64(str.data(), str.size());
        }
        case arrow::Type::FIXED_SIZE_BINARY:
        {
            const auto& str = static_cast<const arrow::FixedSizeBinaryArray&>(ar).GetView(pos);
            return CityHash64(str.data(), str.size());
        }
        case arrow::Type::TIMESTAMP:
            return THash<i64>()(static_cast<const arrow::TimestampArray&>(ar).Value(pos));
        case arrow::Type::TIME32:
            return THash<i32>()(static_cast<const arrow::Time32Array&>(ar).Value(pos));
        case arrow::Type::TIME64:
            return THash<i64>()(static_cast<const arrow::Time64Array&>(ar).Value(pos));
        case arrow::Type::DURATION:
            return THash<i64>()(static_cast<const arrow::DurationArray&>(ar).Value(pos));
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::NA:
        case arrow::Type::DECIMAL256:
        case arrow::Type::DECIMAL:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
            Y_ABORT("not implemented");
            break;
    }
    return 0;
}

ui64 TShardedRecordBatch::GetMemorySize() const {
    return NArrow::GetBatchMemorySize(RecordBatch);
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TShardingSplitIndex::Apply(const std::shared_ptr<arrow::RecordBatch>& input) {
    Y_ABORT_UNLESS(input);
    Y_ABORT_UNLESS(input->num_rows() == RecordsCount);
    auto permutation = BuildPermutation();
    auto resultBatch = NArrow::TStatusValidator::GetValid(arrow::compute::Take(input, *permutation)).record_batch();
    Y_ABORT_UNLESS(resultBatch->num_rows() == RecordsCount);
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    ui64 startIndex = 0;
    for (auto&& i : Remapping) {
        result.emplace_back(resultBatch->Slice(startIndex, i.size()));
        startIndex += i.size();
    }
    return result;
}

NKikimr::NArrow::TShardedRecordBatch TShardingSplitIndex::Apply(const ui32 shardsCount, const std::shared_ptr<arrow::RecordBatch>& input, const std::string& hashColumnName) {
    if (!input) {
        return TShardedRecordBatch();
    }
    if (shardsCount == 1) {
        return TShardedRecordBatch(input);
    }
    auto hashColumn = input->GetColumnByName(hashColumnName);
    if (!hashColumn) {
        return TShardedRecordBatch(input);
    }
    std::optional<TShardingSplitIndex> splitter;
    if (hashColumn->type()->id() == arrow::Type::UINT64) {
        splitter = TShardingSplitIndex::Build<arrow::UInt64Array>(shardsCount, *hashColumn);
    } else if (hashColumn->type()->id() == arrow::Type::UINT32) {
        splitter = TShardingSplitIndex::Build<arrow::UInt32Array>(shardsCount, *hashColumn);
    } else if (hashColumn->type()->id() == arrow::Type::INT64) {
        splitter = TShardingSplitIndex::Build<arrow::Int64Array>(shardsCount, *hashColumn);
    } else if (hashColumn->type()->id() == arrow::Type::INT32) {
        splitter = TShardingSplitIndex::Build<arrow::Int32Array>(shardsCount, *hashColumn);
    } else {
        Y_ABORT_UNLESS(false);
    }
    return TShardedRecordBatch(input, splitter->Apply(input));
}

std::shared_ptr<arrow::UInt64Array> TShardingSplitIndex::BuildPermutation() const {
    arrow::UInt64Builder builder;
    Y_ABORT_UNLESS(builder.Reserve(RecordsCount).ok());

    for (auto&& i : Remapping) {
        for (auto&& idx : i) {
            TStatusValidator::Validate(builder.Append(idx));
        }
    }

    std::shared_ptr<arrow::UInt64Array> out;
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::RecordBatch> ReverseRecords(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto permutation = NArrow::MakePermutation(batch->num_rows(), true);
    return NArrow::TStatusValidator::GetValid(arrow::compute::Take(batch, permutation)).record_batch();
}

}
