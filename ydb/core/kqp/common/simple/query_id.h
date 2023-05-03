#pragma once
#include "settings.h"
#include <util/generic/string.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NKqp {

struct TKqpQueryId {
    TString Cluster;
    TString Database;
    TString UserSid;
    TString Text;
    TKqpQuerySettings Settings;
    NKikimrKqp::EQueryType QueryType;

public:
    TKqpQueryId(const TString& cluster, const TString& database, const TString& text, NKikimrKqp::EQueryType type);

    bool IsSql() const;

    bool operator==(const TKqpQueryId& other) const {
        return
            Cluster == other.Cluster &&
            Database == other.Database &&
            UserSid == other.UserSid &&
            Text == other.Text &&
            Settings == other.Settings &&
            QueryType == other.QueryType;
    }

    bool operator!=(const TKqpQueryId& other) {
        return !(*this == other);
    }

    bool operator<(const TKqpQueryId&) = delete;
    bool operator>(const TKqpQueryId&) = delete;
    bool operator<=(const TKqpQueryId&) = delete;
    bool operator>=(const TKqpQueryId&) = delete;

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(Cluster, Database, UserSid, Text, Settings, QueryType);
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
