#pragma once

#include "events/configs_dispatcher.h"

#include <ydb/core/config/init/init.h>

namespace NKikimr::NConsole {

/**
 * Initial config is used to initilize Configs Dispatcher. All received configs
 * are compared to the current one and notifications are not sent to local
 * subscribers if there is no config modification detected.
 */
IActor *CreateConfigsDispatcher(const NConfig::TConfigsDispatcherInitInfo& initInfo);

} // namespace NKikimr::NConsole
