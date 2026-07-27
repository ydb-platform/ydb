#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/yexception.h>

#include <optional>

namespace NYdb::inline Dev::NRowRangesDetail {

inline std::optional<TResultSet> TryExtractStreamPart(NQuery::TExecuteQueryPart& part) {
    if (!part.HasResultSet()) {
        return std::nullopt;
    }
    if (part.GetResultSetIndex() > 0) {
        ythrow yexception() << "multiple queries in one range is not allowed";
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

//! Drains the next result-set chunk from a stream iterator (sync over async).
//! Returns std::nullopt on end-of-stream (EOS).
//! For ExecuteQuery parts, rejects result_set_index > 0 via TryExtractStreamPart.
template <typename Iterator>
std::optional<TResultSet> DrainStreamIterator(Iterator& iterator) {
    for (;;) {
        auto part = iterator.ReadNext().ExtractValueSync();
        if (!part.IsSuccess()) {
            if (part.EOS()) {
                return std::nullopt;
            }
            TStatus status(std::move(part));
            throw NStatusHelpers::TYdbRangeErrorException(status) << status;
        }
        if (auto extracted = TryExtractStreamPart(part)) {
            return extracted;
        }
    }
}

} // namespace NYdb::inline Dev::NRowRangesDetail
