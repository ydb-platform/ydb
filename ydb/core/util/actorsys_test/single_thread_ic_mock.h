#pragma once

#include "defs.h"

namespace NKikimr {

    class TTestActorSystem;

    class TSingleThreadInterconnectMock {
        struct TNode;

        class TProxyActor;
        class TSessionActor;

    private:
        const ui64 BurstCapacityBytes;
        const ui64 BytesPerSecond;
        TTestActorSystem *TestActorSystem;
        std::unordered_map<ui32, std::shared_ptr<TNode>> Nodes;
        std::unordered_map<std::pair<ui32, ui32>, TProxyActor*, THash<std::pair<ui32, ui32>>> Proxies;

    public:
        TSingleThreadInterconnectMock(ui64 burstCapacityBytes, ui64 bytesPerSecond, TTestActorSystem *tas);
        ~TSingleThreadInterconnectMock();

        std::unique_ptr<NActors::IActor> CreateProxyActor(ui32 nodeId, ui32 peerNodeId,
            TIntrusivePtr<NActors::TInterconnectProxyCommon> common);
    };

}
