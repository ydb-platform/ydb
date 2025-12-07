#pragma once

#include "model_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

struct TOpenAiModelSettings {
    TString BaseUrl;
    TString ModelId;
    TString ApiKey;
};

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient::NAi
