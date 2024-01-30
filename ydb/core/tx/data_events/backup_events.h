#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/backup_events.pb.h>

namespace NKikimr::NEvents {

struct TBackupEvents {
    // @TODO discuss
    enum EEventType {
        EvBackupShardProposeReady = EventSpaceBegin(TKikimrEvents::ES_DATA_OPERATIONS),
        EvBackupShardProposeResult,
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS)");

    struct TEvBackupShardPropose
        : public NActors::TEventPB<TEvBackupShardPropose, NKikimrBackupEvents::TEvBackupShardPropose,
                                   EvBackupShardProposeReady> {
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