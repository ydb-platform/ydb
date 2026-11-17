#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <util/generic/string.h>
#include <util/str_stl.h>
#include <util/string/builder.h>

#include <tuple>

namespace NKikimr::NKqp {

struct TKqpQuerySettings {
    bool DocumentApiRestricted = true;
    bool IsInternalCall = false;
    bool IsAnalyze = false;
    i32 RuntimeParameterSizeLimit = 0;
    bool RuntimeParameterSizeLimitSatisfied = false;

    NKikimrKqp::EQueryType QueryType = NKikimrKqp::EQueryType::QUERY_TYPE_UNDEFINED;
    Ydb::Query::Syntax Syntax = Ydb::Query::Syntax::SYNTAX_UNSPECIFIED;
    bool UsePessimisticLocks = false;

    explicit TKqpQuerySettings(NKikimrKqp::EQueryType queryType)
        : QueryType(queryType) {}

    // Every field, for comparison and for the cache key. The binding names all
    // of them: a field added to the struct and not listed here does not compile.
    auto Fields() const {
        const auto& [documentApiRestricted, isInternalCall, isAnalyze, runtimeParameterSizeLimit,
            runtimeParameterSizeLimitSatisfied, queryType, syntax, usePessimisticLocks] = *this;
        return std::tie(documentApiRestricted, isInternalCall, isAnalyze, runtimeParameterSizeLimit,
            runtimeParameterSizeLimitSatisfied, queryType, syntax, usePessimisticLocks);
    }

    bool operator==(const TKqpQuerySettings& other) const {
        return Fields() == other.Fields();
    }

    bool operator!=(const TKqpQuerySettings& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQuerySettings&) = delete;
    bool operator>(const TKqpQuerySettings&) = delete;
    bool operator<=(const TKqpQuerySettings&) = delete;
    bool operator>=(const TKqpQuerySettings&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(
            DocumentApiRestricted, IsInternalCall, IsAnalyze, QueryType, Syntax,
            UsePessimisticLocks, RuntimeParameterSizeLimitSatisfied);
        return THash<decltype(tuple)>()(tuple);
    }

    // Appends bytes that are equal exactly when operator== is true.
    void AppendCacheKey(TString& out) const {
        std::apply([&out](const auto&... field) {
            (out.append(reinterpret_cast<const char*>(&field), sizeof(field)), ...);
        }, Fields());
    }

    TString SerializeToString() const {
        TStringBuilder result = TStringBuilder() << "{"
            << "DocumentApiRestricted: " << DocumentApiRestricted << ", "
            << "IsInternalCall: " << IsInternalCall << ", "
            << "IsAnalyze: " << IsAnalyze << ", "
            << "QueryType: " << QueryType << ", "
            << "Syntax: " << static_cast<int>(Syntax) << ", "
            << "UsePessimisticLocks: " << UsePessimisticLocks << ", "
            << "RuntimeParameterSizeLimit: " << RuntimeParameterSizeLimit << ", "
            << "RuntimeParameterSizeLimitSatisfied: " << RuntimeParameterSizeLimitSatisfied
            << "}";
        return result;
    }
};

} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQuerySettings> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQuerySettings& settings) const {
        return settings.GetHash();
    }
};
