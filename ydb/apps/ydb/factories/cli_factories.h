#pragma once

#include <ydb/library/yaml_config/public/yaml_config.h>

#include <memory>

namespace NYdb::NConsoleClient {

// A way to parameterize ydb cli binary, we do it via a set of factories
struct TModuleFactories {
    std::shared_ptr<NKikimr::NYamlConfig::IConfigSwissKnife> ConfigSwissKnife;
};

struct TAppData {
    TModuleFactories Factories;
};

TAppData* AppData();

} // namespace NYdb::NConsoleClient
