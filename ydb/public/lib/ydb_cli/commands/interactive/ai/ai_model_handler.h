#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_interface.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/tool_interface.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient::NAi {

class TModelHandler {
    inline const static NColorizer::TColors Colors = NColorizer::AutoColors(Cout);

public:
    struct TSettings {
        TInteractiveConfigurationManager::TAiProfile::TPtr Profile;
        TString Prompt; // Current interactive CLI prompt
        TString Database;
        TDriver Driver;
    };

    TModelHandler(const TSettings& settings, const TInteractiveLogger& log);

    void HandleLine(const TString& input);

    void ClearContext();

private:
    void SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile);

    void SetupTools(const TSettings& settings);

private:
    TInteractiveLogger Log;
    IModel::TPtr Model;
    std::unordered_map<TString, ITool::TPtr> Tools;
};

} // namespace NYdb::NConsoleClient::NAi
