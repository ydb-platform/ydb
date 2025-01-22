#pragma once

#include "fwd.h"

#include "fluent_settings_helpers.h"

namespace NYdb::inline V2 {

enum class ES3Scheme {
    HTTP = 1 /* "http" */,
    HTTPS = 2 /* "https" */,
};

template <typename TDerived>
struct TS3Settings {
    using TSelf = TDerived;

    FLUENT_SETTING_DEPRECATED(TString, Endpoint);
    FLUENT_SETTING_DEFAULT_DEPRECATED(ES3Scheme, Scheme, ES3Scheme::HTTPS);
    FLUENT_SETTING_DEPRECATED(TString, Bucket);
    FLUENT_SETTING_DEPRECATED(TString, AccessKey);
    FLUENT_SETTING_DEPRECATED(TString, SecretKey);
    // true by default for backward compatibility
    FLUENT_SETTING_DEFAULT_DEPRECATED(bool, UseVirtualAddressing, true);
};

} // namespace NYdb
