#include "permutations.h"

#include "arrow_helpers.h"
#include "replace_key.h"
#include "size_calcer.h"
#include "hash/calcer.h"

#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::UInt64Array> MakePermutation(const int size, const bool reverse) {
    arrow::UInt64Builder builder;
    TStatusValidator::Validate(builder.Reserve(size));

    if (size) {
        if (reverse) {
            ui64 value = size - 1;
            for (i64 i = 0; i < size; ++i, --value) {
                TStatusValidator::Validate(builder.Append(value));
            }
        } else {
            for (i64 i = 0; i < size; ++i) {
                TStatusValidator::Validate(builder.Append(i));
            }
        }
    }

    std::shared_ptr<arrow::UInt64Array> out;
    TStatusValidator::Validate(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique) {
    auto keyBatch = TColumnOperator().VerifyIfAbsent().Adapt(batch, sortingKey).DetachResult();
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

template <class TIndex>
std::shared_ptr<arrow::UInt64Array> MakeFilterPermutationImpl(const std::vector<TIndex>& indexes) {
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

std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui32>& indexes) {
    return MakeFilterPermutationImpl(indexes);
}

std::shared_ptr<arrow::UInt64Array> MakeFilterPermutation(const std::vector<ui64>& indexes) {
    return MakeFilterPermutationImpl(indexes);
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

namespace {

template <class TDataContainer>
bool BuildHashUI64Impl(std::shared_ptr<TDataContainer>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    if (fieldNames.size() == 0) {
        return false;
    }
    Y_ABORT_UNLESS(!batch->GetColumnByName(hashFieldName));
    if (fieldNames.size() == 1) {
        auto column = batch->GetColumnByName(fieldNames.front());
        if (!column) {
            AFL_WARN(NKikimrServices::ARROW_HELPER)("event", "cannot_build_hash")("reason", "field_not_found")("field_name", fieldNames.front());
            return false;
        }
        Y_ABORT_UNLESS(column);
        if (column->type()->id() == arrow::Type::UINT64 || column->type()->id() == arrow::Type::UINT32 || column->type()->id() == arrow::Type::INT64 || column->type()->id() == arrow::Type::INT32) {
            batch = TStatusValidator::GetValid(batch->AddColumn(batch->num_columns(), std::make_shared<arrow::Field>(hashFieldName, column->type()), column));
            return true;
        }
    }
    std::shared_ptr<arrow::Array> hashColumn = NArrow::NHash::TXX64(fieldNames, NArrow::NHash::TXX64::ENoColumnPolicy::Verify, 34323543).ExecuteToArray(batch);
    batch = NAdapter::TDataBuilderPolicy<TDataContainer>::AddColumn(batch, std::make_shared<arrow::Field>(hashFieldName, hashColumn->type()), hashColumn);
    return true;
}

}

bool THashConstructor::BuildHashUI64(std::shared_ptr<arrow::Table>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    return BuildHashUI64Impl(batch, fieldNames, hashFieldName);
}

bool THashConstructor::BuildHashUI64(std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    return BuildHashUI64Impl(batch, fieldNames, hashFieldName);
}

ui64 TShardedRecordBatch::GetMemorySize() const {
    return NArrow::GetTableMemorySize(RecordBatch);
}

TShardedRecordBatch::TShardedRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    AFL_VERIFY(batch);
    RecordBatch = TStatusValidator::GetValid(arrow::Table::FromRecordBatches(batch->schema(), {batch}));
}


TShardedRecordBatch::TShardedRecordBatch(const std::shared_ptr<arrow::Table>& batch)
    : RecordBatch(batch)
{
    AFL_VERIFY(RecordBatch);
}

TShardedRecordBatch::TShardedRecordBatch(const std::shared_ptr<arrow::Table>& batch, std::vector<std::vector<ui32>>&& splittedByShards)
    : RecordBatch(batch)
    , SplittedByShards(std::move(splittedByShards))
{
    AFL_VERIFY(RecordBatch);
    AFL_VERIFY(SplittedByShards.size());
}

std::vector<std::shared_ptr<arrow::Table>> TShardingSplitIndex::Apply(const std::shared_ptr<arrow::Table>& input) {
    AFL_VERIFY(input);
    AFL_VERIFY(input->num_rows() == RecordsCount);
    auto permutation = BuildPermutation();
    auto resultBatch = NArrow::TStatusValidator::GetValid(arrow::compute::Take(input, *permutation)).table();
    AFL_VERIFY(resultBatch->num_rows() == RecordsCount);
    std::vector<std::shared_ptr<arrow::Table>> result;
    ui64 startIndex = 0;
    for (auto&& i : Remapping) {
        result.emplace_back(resultBatch->Slice(startIndex, i.size()));
        startIndex += i.size();
    }
    AFL_VERIFY(startIndex == RecordsCount);
    return result;
}

NKikimr::NArrow::TShardedRecordBatch TShardingSplitIndex::Apply(const ui32 shardsCount, const std::shared_ptr<arrow::Table>& input, const std::string& hashColumnName) {
    AFL_VERIFY(input);
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
    auto resultBatch = NArrow::TStatusValidator::GetValid(input->RemoveColumn(input->schema()->GetFieldIndex(hashColumnName)));
    return TShardedRecordBatch(resultBatch, splitter->DetachRemapping());
}

TShardedRecordBatch TShardingSplitIndex::Apply(const ui32 shardsCount, const std::shared_ptr<arrow::RecordBatch>& input, const std::string& hashColumnName) {
    return Apply(shardsCount, TStatusValidator::GetValid(arrow::Table::FromRecordBatches(input->schema(), {input}))
        , hashColumnName);
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
    AFL_VERIFY(batch);
    auto permutation = NArrow::MakePermutation(batch->num_rows(), true);
    return NArrow::TStatusValidator::GetValid(arrow::compute::Take(batch, permutation)).record_batch();
}

std::shared_ptr<arrow::Table> ReverseRecords(const std::shared_ptr<arrow::Table>& batch) {
    AFL_VERIFY(batch);
    auto permutation = NArrow::MakePermutation(batch->num_rows(), true);
    return NArrow::TStatusValidator::GetValid(arrow::compute::Take(batch, permutation)).table();
}

}
