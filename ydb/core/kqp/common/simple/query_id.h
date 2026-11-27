#pragma once
#include "settings.h"

#include <yql/essentials/core/pg_settings/guc_settings.h>

#include <util/digest/numeric.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <map>
#include <memory>
#include <optional>

namespace NKikimr::NKqp {

// GUC settings in the canonical form of the key. Only Encode() makes one, so a key
// cannot be built from another serialization of the same settings.
class TKqpEncodedGUC {
public:
    static TKqpEncodedGUC Encode(const TGUCSettings& gUCSettings);

    TStringBuf Bytes() const {
        return Bytes_;
    }

private:
    explicit TKqpEncodedGUC(TString bytes)
        : Bytes_(std::move(bytes))
    {}

    TString Bytes_;
};

// Compile cache key. Everything that takes part in equality except the text and the
// PG compatibility flag is encoded into one buffer when the key is built, so that the
// comparison done under the cache lock is a hash check and two memcmp calls. The
// buffer starts with the end offsets of its fields, then the raw field bytes follow.
struct TKqpQueryId {
public:
    using TParameterTypes = std::map<TString, Ydb::Type>;

    TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId, const TString& userSid,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        const TGUCSettings& gUCSettings);

    TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        const TGUCSettings& gUCSettings);

    TKqpQueryId(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
        const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
        const TKqpEncodedGUC& encodedGUCSettings);

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

    // Parses the encoded bytes: only for the rare paths that need the settings back.
    TGUCSettings DecodeGUCSettings() const;

    bool DisablePgCompatibility() const {
        return DisablePgCompatibility_;
    }

    // Set by the compile service after the key is built, so it is kept out of Key_.
    void SetDisablePgCompatibility(bool value) {
        DisablePgCompatibility_ = value;
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
        return CombineHashes(Hash_, static_cast<size_t>(DisablePgCompatibility_));
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
    // The arguments may point into Key_: it is replaced only at the end.
    void BuildKey(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
        TStringBuf encodedGUCSettings);

private:
    TString Key_;
    // Kept apart: shares the request's buffer.
    TString Text_;
    // Setters rebuild the key without hashing a long text again.
    size_t TextHash_ = 0;
    size_t Hash_ = 0;
    TKqpQuerySettings Settings_;
    std::shared_ptr<const TParameterTypes> QueryParameterTypes_;
    bool DisablePgCompatibility_ = true;
};
// Keeps the encoded bytes while the settings stay equal to the ones they were
// encoded from: comparing the settings is much cheaper than encoding them.
class TKqpEncodedGUCSettings {
public:
    const TKqpEncodedGUC& Get(const TGUCSettings& gUCSettings);

private:
    std::optional<std::pair<TGUCSettings, TKqpEncodedGUC>> Cache_;
};

} // namespace NKikimr::NKqp

template<>
struct THash<NKikimr::NKqp::TKqpQueryId> {
    inline size_t operator()(const NKikimr::NKqp::TKqpQueryId& query) const {
        return query.GetHash();
    }
};
