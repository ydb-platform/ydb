#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/backup_events.pb.h>

namespace NKikimr::NEvents {

struct TBackupEvents {
    enum EEventType {
        EvBackupShardBatchPersist = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SHARD),
        EvBackupShardBatchPersistResult,
        EvBackupShardResult,
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SHARD)");

    class TEvBackupShardBatchPersist
        : public NActors::TEventPB<TEvBackupShardBatchPersist, NKikimrBackupEvents::TEvBackupShardBatchPersist,
                                   EvBackupShardBatchPersist> {
    public:
        TEvBackupShardBatchPersist() = default;
    };

    class TEvBackupShardBatchPersistResult
        : public NActors::TEventPB<TEvBackupShardBatchPersistResult, NKikimrBackupEvents::TEvBackupShardBatchPersistResult,
                                   EvBackupShardBatchPersistResult> {
    public:
        TEvBackupShardBatchPersistResult() = default;
    };

    class TEvBackupShardResult
        : public NActors::TEventPB<TEvBackupShardResult, NKikimrBackupEvents::TEvBackupShardResult,
                                   EvBackupShardResult> {
    public:
        TEvBackupShardResult() = default;
    };
};

}   // namespace NKikimr::NEvents