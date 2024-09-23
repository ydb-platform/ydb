#include "permutations.h"

#include "arrow_helpers.h"
#include "size_calcer.h"
#include "hash/calcer.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/formats/arrow/common/validation.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NArrow {

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::vector<std::shared_ptr<arrow::Array>>& keyColumns, const bool andUnique) {
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

    if (haveNulls) {
        std::sort(points.begin(), points.end());
    } else {
        std::sort(points.begin(), points.end(), [](const TRawReplaceKey& a, const TRawReplaceKey& b) {
            return a.CompareNotNull(b) == std::partial_ordering::less;
        });
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

std::shared_ptr<arrow::UInt64Array> MakeSortPermutation(const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& sortingKey, const bool andUnique) {
    auto keyBatch = TColumnOperator().VerifyIfAbsent().Adapt(batch, sortingKey).DetachResult();
    return MakeSortPermutation(keyBatch->columns(), andUnique);
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

}
