#include "client_config.h"

#include <yt/yql/providers/yt/common/yql_yt_settings.h>

namespace NYql {

NYT::TConfigPtr CreateYtClientConfig(const TYtSettings& settings) {
    auto config = MakeIntrusive<NYT::TConfig>(*NYT::TConfig::Get());
    config->LockFileStorage = settings._EnableFileCacheLock.Get().GetOrElse(false);
    return config;
}

} // namespace NYql
