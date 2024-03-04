#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/backup_events.pb.h>

namespace NKikimr::NEvents {

struct TBackupEvents {
    enum EEventType {
        EvBackupShardResult = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SHARD),
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD)");

    class TEvBackupShardResult
        : public NActors::TEventPB<TEvBackupShardResult, NKikimrBackupEvents::TEvBackupShardResult,
                                   EvBackupShardResult> {
    public:
        TEvBackupShardResult() = default;
    };
};

}   // namespace NKikimr::NEvents