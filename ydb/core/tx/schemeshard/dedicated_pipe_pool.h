#pragma once

#include "defs.h"
#include "schemeshard_identificators.h"

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/executor_thread.h>

#include <util/generic/map.h>

namespace NKikimr::NSchemeShard {

template <typename TEntityId>
class TDedicatedPipePool {
    TMap<TEntityId, TMap<TTabletId, TActorId>> Pipes;
    TMap<TActorId, std::pair<TEntityId, TTabletId>> Owners;

public:
    void Create(const TEntityId& entityId, TTabletId dst, THolder<IEventBase> message, const TActorContext& ctx) {
        Y_ABORT_UNLESS(!Pipes[entityId].contains(dst));
        using namespace NTabletPipe;

        const auto clientId = ctx.ExecutorThread.RegisterActor(CreateClient(ctx.SelfID, ui64(dst), TClientRetryPolicy {
            .MinRetryTime = TDuration::MilliSeconds(100),
            .MaxRetryTime = TDuration::Seconds(30),
        }));

        Pipes[entityId][dst] = clientId;
        Owners[clientId] = std::make_pair(entityId, dst);

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

    ui64 CloseAll(const TEntityId& entityId, const TActorContext& ctx) {
        auto entityIt = Pipes.find(entityId);
        if (entityIt == Pipes.end()) {
            return 0;
        }

        const auto& entityPipes = entityIt->second;
        TVector<TTabletId> tablets(Reserve(entityPipes.size()));

        for (const auto& [tabletId, _] : entityPipes) {
            tablets.push_back(tabletId);
        }

        for (const auto& tabletId : tablets) {
            Close(entityId, tabletId, ctx);
        }

        return tablets.size();
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
