#pragma once

#include <ydb/core/backup/common/table_checksum_events.h>
#include <ydb/library/actors/core/actor.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

class TImportChecksum {
public:
    NActors::TActorId EnsureActor(ui64 operationId, const NActors::TActorContext& ctx);
    THolder<NActors::IEventHandle> StopOperation(ui64 operationId, NActors::TActorId sender);
    void OnIdle(const NBackup::TEvTableChecksum::TEvImportChecksumIdle::TPtr& ev, const NActors::TActorContext& ctx);
    void Shutdown(const NActors::TActorContext& ctx);

    NActors::TActorId GetActor() const {
        return Actor;
    }

    NBackup::TEvTableChecksum::TOwnerPtr GetOwner();

private:
    NBackup::TEvTableChecksum::TOwnerPtr Owner;
    NActors::TActorId Actor;
    THashSet<ui64> Operations;
    ui64 IdleCookie = 0;
};

} // namespace NKikimr::NSchemeShard
