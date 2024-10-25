#pragma once
#include "settings.h"

#include <ydb/library/yql/core/pg_settings/guc_settings.h>

#include <util/generic/string.h>

#include <map>
#include <memory>

namespace NKikimr::NKqp {

struct TKqpQueryId {
    TString Cluster;
    TString Database;
    TString DatabaseId;
    TString UserSid;
    TString Text;
    TKqpQuerySettings Settings;

    std::shared_ptr<std::map<TString, Ydb::Type>> QueryParameterTypes;
    TGUCSettings GUCSettings;

public:
    TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId, const TString& text,
        const TKqpQuerySettings& settings, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameterTypes,
        const TGUCSettings& gUCSettings);

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
        auto tuple = std::make_tuple(Cluster, Database, UserSid, Text, Settings,
            QueryParameterTypes ? QueryParameterTypes->size() : 0u,
            GUCSettings.GetHash());
        return THash<decltype(tuple)>()(tuple);
    }

    TString SerializeToString() const;
};
} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQueryId> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQueryId& query) const {
        return query.GetHash();
    }
};
