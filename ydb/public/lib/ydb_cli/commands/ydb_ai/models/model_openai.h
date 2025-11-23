#pragma once

#include "model_interface.h"

#include <util/generic/string.h>

namespace NYdb::NConsoleClient::NAi {

struct TOpenAiModelSettings {
    TString BaseUrl;  // AI-TODO KIKIMR-24211 add default value
    std::optional<TString> ModelId;
    TString ApiKey;
};

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
