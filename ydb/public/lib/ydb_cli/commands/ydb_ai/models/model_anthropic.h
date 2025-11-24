#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

struct TAnthropicModelSettings {
    TString BaseUrl;
    TString ModelId;
    std::optional<TString> ApiKey;
};

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings, const TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient::NAi
