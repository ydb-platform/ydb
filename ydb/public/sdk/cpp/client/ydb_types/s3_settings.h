#pragma once

#include "fluent_settings_helpers.h"

namespace NYdb {

enum class ES3Scheme {
    HTTP = 1 /* "http" */,
    HTTPS = 2 /* "https" */,
};

template <typename TDerived>
struct TS3Settings {
    using TSelf = TDerived;

    FLUENT_SETTING(TString, Endpoint);
    FLUENT_SETTING_DEFAULT(ES3Scheme, Scheme, ES3Scheme::HTTPS);
    FLUENT_SETTING(TString, Bucket);
    FLUENT_SETTING(TString, AccessKey);
    FLUENT_SETTING(TString, SecretKey);
    // true by default for backward compatibility
    FLUENT_SETTING_DEFAULT(bool, UseVirtualAddressing, true);
};

} // namespace NYdb
