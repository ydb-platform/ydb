#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>
#include <functional>

namespace NYdb {

struct TCommonClientSettings {
    using TSelf = TCommonClientSettings;

    //! NOTE: Options below are discovery state specific options.
    //! If client overrides it and no state in cache (no client with same settings)
    //! ctor may blocks during discovery (according to DiscoveryMode settings
    //! The key of state cache is <Database, Credentials, DiscoveryEndpoint, DiscoveryMode> tuple
    //! For performance reason (to avoid extra discovery requests) it is
    //! always better to keep instance of client.

    //! Allows to override current database for client
    FLUENT_SETTING_OPTIONAL(TStringType, Database);
    //! Allows to override current discovery endpoint
    FLUENT_SETTING_OPTIONAL(TStringType, DiscoveryEndpoint);
    //! Allows to override current token for client
    TSelf& AuthToken(const TMaybe<TStringType>& token);
    //! Allows to override current credentials provider
    FLUENT_SETTING_OPTIONAL(std::shared_ptr<ICredentialsProviderFactory>, CredentialsProviderFactory);
    //! Allows to override discovery mode
    FLUENT_SETTING_OPTIONAL(EDiscoveryMode, DiscoveryMode);
    //! Allows to override current Ssl mode
    FLUENT_SETTING_OPTIONAL(bool, EnableSsl);
    //! Allows to override current Ssl cert
    FLUENT_SETTING_OPTIONAL(TStringType, CaCert);
};

template<class TDerived>
struct TCommonClientSettingsBase : public TCommonClientSettings {

#define COMMON_CLIENT_SETTINGS_TO_DERIVED(type, name) \
    TDerived& name(const type& value) { \
        TCommonClientSettings::name(value); \
        return static_cast<TDerived&>(*this); \
    }

    COMMON_CLIENT_SETTINGS_TO_DERIVED(TStringType, Database);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(TStringType, DiscoveryEndpoint);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(TMaybe<TStringType>, AuthToken);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(std::shared_ptr<ICredentialsProviderFactory>, CredentialsProviderFactory);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(EDiscoveryMode, DiscoveryMode);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(bool, EnableSsl);
    COMMON_CLIENT_SETTINGS_TO_DERIVED(TStringType, CaCert);

#undef COMMON_CLIENT_SETTINGS_TO_DERIVED

};

TCommonClientSettings GetClientSettingsFromConnectionString(const TStringType& connectionString);

} // namespace NYdb
