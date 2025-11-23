#pragma once

#include "model_interface.h"

namespace NYdb::NConsoleClient::NAi {

struct TAnthropicModelSettings {
    TString BaseUrl;  // AI-TODO KIKIMR-24211 add default value
    TString ModelId;
    TString ApiKey;
    ui64 MaxTokens;
};

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
