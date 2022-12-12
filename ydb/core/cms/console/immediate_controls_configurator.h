#pragma once
#include "defs.h"

#include <ydb/core/control/immediate_control_board_impl.h>

#include <util/generic/ptr.h>

namespace NKikimr::NConsole {

/**
 * Immediate Controls Configurator is used to work with
 * immediate control board via CMS.
 */
IActor *CreateImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                            const NKikimrConfig::TImmediateControlsConfig &cfg,
                                            bool allowExistingControls = false);

} // namespace NKikimr::NConsole
