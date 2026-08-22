#pragma once

#include "model_interface.h"

namespace NYdb::NConsoleClient::NAi {

struct TOpenAiModelSettings {
    TString BaseUrl;
    TString ModelId;
    TString ApiKey;
    TString SystemPrompt;
};

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
