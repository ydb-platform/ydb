#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/backup_events.pb.h>

namespace NKikimr::NEvents {

struct TBackupEvents {
    // @TODO discuss
    enum EEventType {
        EvBackupShardPropose = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SHARD),
        EvBackupShardProposeResult,
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD)");

    // @TODO TEvBackupShardPropose useless. Need only result.
    struct TEvBackupShardPropose
        : public NActors::TEventPB<TEvBackupShardPropose, NKikimrBackupEvents::TEvBackupShardPropose,
                                   EvBackupShardPropose> {
        TEvBackupShardPropose() = default;

        TActorId GetSource() const {
            return ActorIdFromProto(Record.GetSource());
        }
    };

    struct TEvBackupShardProposeResult
        : public NActors::TEventPB<TEvBackupShardProposeResult, NKikimrBackupEvents::TEvBackupShardProposeResult,
                                   EvBackupShardProposeResult> {
        TEvBackupShardProposeResult() = default;
    };
};

}   // namespace NKikimr::NEvents