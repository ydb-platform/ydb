#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient::NAi {

struct TAnthropicModelSettings {
    TString BaseUrl;
    TString ModelId;
    TString ApiKey;
    TString SystemPrompt;
};

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient::NAi
