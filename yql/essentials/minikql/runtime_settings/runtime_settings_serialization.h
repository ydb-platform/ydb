#pragma once

#include "runtime_settings.h"
#include "runtime_settings_hash.h"

#include <memory>

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

// Immutable execution metadata only: parameters, credentials and activation state
// belong to the request. Resolve accepts already activated settings, just like
// DeserializeRuntimeSettingsFromProto, and never publishes its mutable dispatcher.
struct TResolvedRuntimeSettings {
    const TRuntimeSettings::TConstPtr Settings;
    const TRuntimeSettingsStableHash StableHash;
};

std::shared_ptr<const TResolvedRuntimeSettings> ResolveRuntimeSettings(
    const NProto::TRuntimeSettings& proto);

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials,
    const TQContext& qContext,
    std::function<void(const TString&)> onPartialFeatureActivation);

TRuntimeSettings::TPtr DeserializeRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto);

NProto::TRuntimeSettings SerializeRuntimeSettingsToProto(
    const TRuntimeSettings& config);

TString SerializeRuntimeSettingsToString(const TRuntimeSettings& config);

TRuntimeSettings::TPtr CreateRuntimeSettingsFromString(
    const TString& data);

} // namespace NYql
