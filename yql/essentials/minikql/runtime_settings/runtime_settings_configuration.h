#pragma once

#include "runtime_settings.h"

#include <yql/essentials/providers/common/config/yql_dispatch.h>

namespace NYql {

class TRuntimeSettingsConfiguration: public NCommon::TDispatcher<TRuntimeSettings> {
public:
    static const TRuntimeSettingsConfiguration& Get();

private:
    TRuntimeSettingsConfiguration();
};

} // namespace NYql
