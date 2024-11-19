#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <util/generic/string.h>
#include <util/str_stl.h>
#include <util/string/builder.h>

#include <tuple>

namespace NKikimr::NKqp {

struct TKqpQuerySettings {
    bool DocumentApiRestricted = true;
    bool IsInternalCall = false;
    NKikimrKqp::EQueryType QueryType = NKikimrKqp::EQueryType::QUERY_TYPE_UNDEFINED;
    Ydb::Query::Syntax Syntax = Ydb::Query::Syntax::SYNTAX_UNSPECIFIED;

    explicit TKqpQuerySettings(NKikimrKqp::EQueryType queryType)
        : QueryType(queryType) {}

    bool operator==(const TKqpQuerySettings& other) const {
        return
            DocumentApiRestricted == other.DocumentApiRestricted &&
            IsInternalCall == other.IsInternalCall &&
            QueryType == other.QueryType &&
            Syntax == other.Syntax;
    }

    bool operator!=(const TKqpQuerySettings& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQuerySettings&) = delete;
    bool operator>(const TKqpQuerySettings&) = delete;
    bool operator<=(const TKqpQuerySettings&) = delete;
    bool operator>=(const TKqpQuerySettings&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(DocumentApiRestricted, IsInternalCall, QueryType, Syntax);
        return THash<decltype(tuple)>()(tuple);
    }

    TString SerializeToString() const {
        TStringBuilder result = TStringBuilder() << "{"
            << "DocumentApiRestricted: " << DocumentApiRestricted << ", "
            << "IsInternalCall: " << IsInternalCall << ", "
            << "QueryType: " << QueryType << "}";
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
