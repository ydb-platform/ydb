#pragma once
#include "settings.h"
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/string.h>

#include <map>
#include <memory>

namespace NKikimr::NKqp {

struct TKqpQueryId {
    TString Cluster;
    TString Database;
    TString UserSid;
    TString Text;
    TKqpQuerySettings Settings;
    NKikimrKqp::EQueryType QueryType;
    std::shared_ptr<std::map<TString, Ydb::Type>> QueryParameterTypes;

public:
    TKqpQueryId(const TString& cluster, const TString& database, const TString& text, NKikimrKqp::EQueryType type, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameterTypes);

    bool IsSql() const;

    bool operator==(const TKqpQueryId& other) const;

    bool operator!=(const TKqpQueryId& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQueryId&) = delete;
    bool operator>(const TKqpQueryId&) = delete;
    bool operator<=(const TKqpQueryId&) = delete;
    bool operator>=(const TKqpQueryId&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(Cluster, Database, UserSid, Text, Settings, QueryType, QueryParameterTypes ? QueryParameterTypes->size() : 0u);
        return THash<decltype(tuple)>()(tuple);
    }
};
} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQueryId> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQueryId& query) const {
        return query.GetHash();
    }
};
