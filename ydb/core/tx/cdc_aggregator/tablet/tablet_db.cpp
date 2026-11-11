#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

void TCdcAggregator::PersistStreamChanges(NIceDb::TNiceDb& db, const TStreamChanges& changes) {
    for (const auto dataShardId : changes.ErasedSources) {
        db.Table<Schema::Sources>().Key(dataShardId).Delete();
    }

    for (const auto dataShardId : changes.UpsertedSources) {
        if (const auto& lastHeartbeat = Stream.GetSources().at(dataShardId)) {
            db.Table<Schema::Sources>().Key(dataShardId).Update(
                NIceDb::TUpdate<Schema::Sources::LastHeartbeatStep>(lastHeartbeat->Step),
                NIceDb::TUpdate<Schema::Sources::LastHeartbeatTxId>(lastHeartbeat->TxId));
        } else {
            db.Table<Schema::Sources>().Key(dataShardId).UpdateToNull<
                Schema::Sources::LastHeartbeatStep,
                Schema::Sources::LastHeartbeatTxId>();
        }
    }

    for (const auto& version : changes.ErasedHeartbeats) {
        db.Table<Schema::Heartbeats>().Key(version.Step, version.TxId).Delete();
    }

    for (const auto& [version, data] : changes.UpsertedHeartbeats) {
        db.Table<Schema::Heartbeats>().Key(version.Step, version.TxId).Update(
            NIceDb::TUpdate<Schema::Heartbeats::Data>(data));
    }

    if (changes.CurrentHeartbeatChanged) {
        const auto& currentHeartbeat = *Stream.GetCurrentHeartbeat();
        db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatStep>(currentHeartbeat.Version.Step),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatTxId>(currentHeartbeat.Version.TxId),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatData>(currentHeartbeat.Data));
    }
}

} // namespace NKikimr::NCdcAggregator
