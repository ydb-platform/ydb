#pragma once

#include "model_interface.h"

namespace NYdb::NConsoleClient::NAi {

struct TAnthropicModelSettings {
    TString BaseUrl;
    TString ModelId;
    TString ApiKey;
    TString SystemPrompt;
};

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
