#pragma once

#include "defs.h"
#include "schemeshard_identificators.h"

#include <ydb/core/base/tablet_pipe.h>

#include <util/generic/map.h>

namespace NKikimr::NSchemeShard {

template <typename TEntityId>
class TDedicatedPipePool {
    TMap<TEntityId, TMap<TTabletId, TActorId>> Pipes;
    TMap<TActorId, std::pair<TEntityId, TTabletId>> Owners;

public:
    void Send(const TEntityId& entityId, TTabletId dst, THolder<IEventBase> message, const TActorContext& ctx) {
        using namespace NTabletPipe;

        if (!Pipes[entityId].contains(dst)) {
            const auto clientId = ctx.Register(CreateClient(ctx.SelfID, ui64(dst), TClientRetryPolicy {
                .MinRetryTime = TDuration::MilliSeconds(100),
                .MaxRetryTime = TDuration::Seconds(30),
            }));

            Pipes[entityId][dst] = clientId;
            Owners[clientId] = std::make_pair(entityId, dst);
        }

        const auto clientId = Pipes[entityId][dst];
        Y_ABORT_UNLESS(Owners[clientId] == std::make_pair(entityId, dst));
        SendData(ctx.SelfID, clientId, message.Release(), 0);
    }

    void Close(const TEntityId& entityId, TTabletId dst, const TActorContext& ctx) {
        auto entityIt = Pipes.find(entityId);
        if (entityIt == Pipes.end()) {
            return;
        }

        auto& entityPipes = entityIt->second;

        auto tabletIt = entityPipes.find(dst);
        if (tabletIt == entityPipes.end()) {
            return;
        }

        const auto& clientId = tabletIt->second;
        NTabletPipe::CloseClient(ctx, clientId);
        Owners.erase(clientId);

        entityPipes.erase(tabletIt);
        if (entityPipes.empty()) {
            Pipes.erase(entityIt);
        }
    }

    void CloseAll(const TEntityId& entityId, const TActorContext& ctx) {
        auto entityIt = Pipes.find(entityId);
        if (entityIt == Pipes.end()) {
            return;
        }

        const auto& entityPipes = entityIt->second;
        TVector<TTabletId> tablets(Reserve(entityPipes.size()));

        for (const auto& [tabletId, _] : entityPipes) {
            tablets.push_back(tabletId);
        }

        for (const auto& tabletId : tablets) {
            Close(entityId, tabletId, ctx);
        }

        return;
    }

    void Shutdown(const TActorContext& ctx) {
        for (const auto& [clientId, _] : Owners) {
            NTabletPipe::CloseClient(ctx, clientId);
        }

        Pipes.clear();
        Owners.clear();
    }

    bool Has(const TActorId& clientId) const {
        return Owners.contains(clientId);
    }

    const TEntityId& GetOwnerId(const TActorId& clientId) const {
        auto it = Owners.find(clientId);
        Y_ABORT_UNLESS(it != Owners.end());
        return it->second.first;
    }

    TTabletId GetTabletId(const TActorId& clientId) const {
        auto it = Owners.find(clientId);
        Y_ABORT_UNLESS(it != Owners.end());
        return it->second.second;
    }
};

}
