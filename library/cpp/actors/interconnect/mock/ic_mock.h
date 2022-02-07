#pragma once

#include <library/cpp/actors/core/actor.h>

#include <library/cpp/actors/interconnect/interconnect_common.h>

namespace NActors {

    class TInterconnectMock {
        class TImpl;
        std::unique_ptr<TImpl> Impl;

    public:
        TInterconnectMock();
        ~TInterconnectMock();
        IActor *CreateProxyMock(ui32 nodeId, ui32 peerNodeId, TInterconnectProxyCommon::TPtr common);
    };

} // NActors
