#pragma once
#include "defs.h"

#include <ydb/library/actors/interconnect/interconnect_common.h>

namespace NKikimr::NConsole {

    /**
     * Interconnect configurator tracks interconnect config changes and applies the part of them that
     * the running interconnect is able to pick up without a restart.
     *
     * Right now that is only TInterconnectConfig.V2Config.Enable, which gates negotiation of the v2
     * session during the handshake. Established sessions are left alone; the new value takes effect
     * for handshakes made from now on. Everything else in the interconnect config -- including the
     * size and shape of the v2 io_uring engine -- is read once at startup and still needs a restart.
     */
    IActor *CreateInterconnectConfigurator(TIntrusivePtr<NActors::TInterconnectProxyCommon> common);

} // namespace NKikimr::NConsole
