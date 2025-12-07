#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/ai/models/model_interface.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient::NAi {

class TModelHandler {
    inline const static NColorizer::TColors Colors = NColorizer::AutoColors(Cout);

public:
    TModelHandler(TInteractiveConfigurationManager::TAiProfile::TPtr profile, const TInteractiveLogger& log);

    void HandleLine(const TString& input);

    void ClearContext();

private:
    void SetupModel(TInteractiveConfigurationManager::TAiProfile::TPtr profile);

private:
    TInteractiveLogger Log;
    IModel::TPtr Model;
};

} // namespace NYdb::NConsoleClient::NAi
