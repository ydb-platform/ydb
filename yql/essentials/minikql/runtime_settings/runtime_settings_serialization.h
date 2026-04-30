#pragma once

#include "runtime_settings_configuration.h"

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/core/credentials/yql_credentials.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYql {

TRuntimeSettings::TPtr CreateRuntimeSettingsFromProto(
    const NProto::TRuntimeSettings& proto,
    const TString& userName,
    TCredentials::TPtr credentials);

NProto::TRuntimeSettings SerializeRuntimeSettingsToProto(
    const TRuntimeSettings& config);

TString SerializeRuntimeSettingsToString(const TRuntimeSettings& config);

TRuntimeSettings::TPtr CreateRuntimeSettingsFromString(
    const TString& data,
    const TString& userName,
    TCredentials::TPtr credentials);

TRuntimeSettings::TPtr CreateRuntimeSettingsFromString(
    const TString& data);

} // namespace NYql
