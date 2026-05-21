#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <optional>

namespace NYdb::inline Dev::NResultRangesDetail {

inline std::optional<TResultSet> TryExtractStreamPart(NQuery::TExecuteQueryPart& part) {
    if (!part.HasResultSet()) {
        return std::nullopt;
    }
    return std::make_optional(part.ExtractResultSet());
}

inline std::optional<TResultSet> TryExtractStreamPart(NTable::TScanQueryPart& part) {
    if (!part.HasResultSet()) {
        return std::nullopt;
    }
    return std::make_optional(part.ExtractResultSet());
}

inline std::optional<TResultSet> TryExtractStreamPart(NTable::TReadTableResultPart& part) {
    return std::make_optional(part.ExtractPart());
}

//! Drains one logical result set from a stream iterator (sync over async).
//! Returns std::nullopt on end-of-stream (EOS).
template <typename Iterator>
std::optional<TResultSet> DrainStreamIterator(Iterator& iterator) {
    for (;;) {
        auto part = iterator.ReadNext().ExtractValueSync();
        if (!part.IsSuccess()) {
            if (part.EOS()) {
                return std::nullopt;
            }
            TStatus status(std::move(part));
            throw NStatusHelpers::TYdbErrorException(status) << status;
        }
        if (auto extracted = TryExtractStreamPart(part)) {
            return extracted;
        }
    }
}

} // namespace NYdb::inline Dev::NResultRangesDetail
