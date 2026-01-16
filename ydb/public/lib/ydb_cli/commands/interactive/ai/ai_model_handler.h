#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_interface.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/tools/tool_interface.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>

#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <util/generic/fwd.h>
#include <util/stream/output.h>

#include <functional>

namespace NYdb::NConsoleClient::NAi {

class TModelHandler {
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

public:
    struct TSettings {
        TInteractiveConfigurationManager::TAiProfile::TPtr Profile;
        TString Prompt; // Current interactive CLI prompt
        TString Database;
        TDriver Driver;
        TString ConnectionString;
    };

    explicit TModelHandler(const TSettings& settings);

    void HandleLine(const TString& input, std::function<void()> onStartWaiting = {}, std::function<void()> onFinishWaiting = {}, std::function<double()> getThinkingTime = {});

    void ClearContext();

private:
    IModel::TToolResponse CallTool(const IModel::TResponse::TToolCall& toolCall, std::vector<TString>& userMessages, bool& interrupted) const;

    void SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile, const TSettings& settings);

    void SetupTools(const TSettings& settings);

private:
    IModel::TPtr Model;
    std::unordered_map<TString, ITool::TPtr> Tools;
};

} // namespace NYdb::NConsoleClient::NAi
