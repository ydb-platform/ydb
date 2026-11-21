#pragma once
#include "settings.h"

#include <yql/essentials/core/pg_settings/guc_settings.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <map>
#include <memory>
#include <optional>

namespace NKikimr::NKqp {

// Compile cache key. Everything that takes part in equality except the text is
// encoded into one buffer when the key is built, so that the comparison done under
// the cache lock is a hash check and two memcmp calls. The buffer starts with the
// end offsets of its fields, then the raw field bytes follow.
struct TKqpQueryId {
public:
    using TParameterTypes = std::map<TString, Ydb::Type>;

    TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId, const TString& userSid,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        const TGUCSettings& gUCSettings);

    TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        const TGUCSettings& gUCSettings);

    static TString EncodeGUCSettings(const TGUCSettings& gUCSettings);

    TKqpQueryId(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        TStringBuf encodedGUCSettings);

    TStringBuf Cluster() const {
        return Field(FieldCluster);
    }

    TStringBuf Database() const {
        return Field(FieldDatabase);
    }

    TStringBuf DatabaseId() const {
        return Field(FieldDatabaseId);
    }

    TStringBuf UserSid() const {
        return Field(FieldUserSid);
    }

    const TString& Text() const {
        return Text_;
    }

    const TKqpQuerySettings& Settings() const {
        return Settings_;
    }

    const std::shared_ptr<const TParameterTypes>& QueryParameterTypes() const {
        return QueryParameterTypes_;
    }

    void SetUserSid(TStringBuf userSid);
    void SetQueryParameterTypes(std::shared_ptr<const TParameterTypes> queryParameterTypes);

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
        return Hash_;
    }

    TString SerializeToString() const;

private:
    enum EField : size_t {
        FieldCluster,
        FieldDatabase,
        FieldDatabaseId,
        FieldUserSid,
        FieldSettings,
        FieldGUCSettings,
        FieldCount,
    };

    TStringBuf Field(EField field) const;

private:
    TString Key_;
    // Kept apart: shares the request's buffer.
    TString Text_;
    size_t Hash_ = 0;
    TKqpQuerySettings Settings_;
    std::shared_ptr<const TParameterTypes> QueryParameterTypes_;
};
// Keeps the encoded bytes while the settings stay equal to the ones they were
// encoded from: comparing the settings is much cheaper than encoding them.
class TKqpEncodedGUCSettings {
public:
    TStringBuf Get(const TGUCSettings& gUCSettings);

private:
    std::optional<TGUCSettings> Source_;
    TString Encoded_;
};

} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQueryId> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQueryId& query) const {
        return query.GetHash();
    }
};
