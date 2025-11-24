#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

struct TOpenAiModelSettings {
    TString BaseUrl;
    std::optional<TString> ModelId;
    std::optional<TString> ApiKey;
};

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings, const TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient::NAi
