#pragma once

#include "runtime_settings_configuration.h"

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/core/credentials/yql_credentials.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

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
