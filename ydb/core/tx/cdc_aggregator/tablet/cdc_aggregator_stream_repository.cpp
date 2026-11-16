#include "cdc_aggregator_stream_repository.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

namespace NKikimr::NCdcAggregator {

void TCdcAggregatorStreamRepository::InsertCdcAggregatorStream(NIceDb::TNiceDb& db, const TCdcAggregatorStream& stream) const {
    db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
        NIceDb::TUpdate<Schema::State::TrackingConfig>(Converter.SerializeTopic(stream.GetTopic())));

    for (const auto& [dataShardId, _] : stream.GetLastHeartbeats()) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }
}

void TCdcAggregatorStreamRepository::ApplyCdcAggregatorStreamSourcesDiff(NIceDb::TNiceDb& db, const TCdcAggregatorStreamSourcesDiff& diff) const {
    for (const auto dataShardId : diff.Deleted) {
        db.Table<Schema::Sources>().Key(dataShardId).Delete();
    }

    for (const auto dataShardId : diff.Inserted) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }
}

void TCdcAggregatorStreamRepository::ApplyCdcAggregatorStreamHeartbeatsDiff(NIceDb::TNiceDb& db, const TCdcAggregatorStreamHeartbeatsDiff& diff) const {
    for (const auto& [dataShardId, heartbeat] : diff.LastHeartbeats) {
        db.Table<Schema::Sources>().Key(dataShardId).Update(
            NIceDb::TUpdate<Schema::Sources::LastHeartbeatStep>(heartbeat.Version.Step),
            NIceDb::TUpdate<Schema::Sources::LastHeartbeatTxId>(heartbeat.Version.TxId),
            NIceDb::TUpdate<Schema::Sources::LastHeartbeatData>(heartbeat.Data));
    }

    if (const auto& heartbeat = diff.CurrentHeartbeat) {
        db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatStep>(heartbeat->Version.Step),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatTxId>(heartbeat->Version.TxId),
            NIceDb::TUpdate<Schema::State::CurrentHeartbeatData>(heartbeat->Data));
    }
}

void TCdcAggregatorStreamRepository::InsertCdcAggregatorStreamPartitioningChange(NIceDb::TNiceDb& db, const TCdcAggregatorStreamPartitioningChange& change) const {
    db.Table<Schema::PartitioningChanges>().Key(change.ParentDataShardId, change.OperationId).Update(
        NIceDb::TUpdate<Schema::PartitioningChanges::ChildDataShardIds>(change.ChildDataShardIds));
}

std::pair<bool, std::optional<TCdcAggregatorStream>> TCdcAggregatorStreamRepository::LoadCdcAggregatorStream(NIceDb::TNiceDb& db) const {
    auto state = db.Table<Schema::State>().Key(Schema::State::Id::Value).Select();
    if (!state.IsReady()) {
        return {false, std::nullopt};
    }
    if (!state.IsValid() || !state.HaveValue<Schema::State::TrackingConfig>()) {
        return {true, std::nullopt};
    }

    auto lastHeartbeats = LoadLastHeartbeats(db);
    if (!lastHeartbeats) {
        return {false, std::nullopt};
    }
    auto changes = LoadPartitioningChanges(db);
    if (!changes) {
        return {false, std::nullopt};
    }

    NKikimrCdcAggregator::TCdcAggregatorStreamTopic proto;
    Y_ENSURE(proto.ParseFromString(state.GetValue<Schema::State::TrackingConfig>()), "Malformed tracking config");
    auto [topic, error] = Converter.ParseTopic(proto);
    Y_ENSURE(topic, "Invalid tracking config: " << error);

    std::optional<NPQ::THeartbeat> currentHeartbeat;
    if (state.HaveValue<Schema::State::CurrentHeartbeatStep>()) {
        currentHeartbeat = NPQ::THeartbeat{
            .Version = TRowVersion(state.GetValue<Schema::State::CurrentHeartbeatStep>(), state.GetValue<Schema::State::CurrentHeartbeatTxId>()),
            .Data = state.GetValue<Schema::State::CurrentHeartbeatData>(),
        };
    }

    return {true, TCdcAggregatorStream(std::move(*topic), std::move(*lastHeartbeats), std::move(currentHeartbeat), std::move(*changes))};
}

std::optional<TCdcAggregatorStream::TLastHeartbeats> TCdcAggregatorStreamRepository::LoadLastHeartbeats(NIceDb::TNiceDb& db) const {
    auto rowset = db.Table<Schema::Sources>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }

    TCdcAggregatorStream::TLastHeartbeats lastHeartbeats;
    while (!rowset.EndOfSet()) {
        auto& lastHeartbeat = lastHeartbeats[rowset.GetValue<Schema::Sources::DataShardId>()];
        if (rowset.HaveValue<Schema::Sources::LastHeartbeatStep>()) {
            lastHeartbeat = NPQ::THeartbeat{
                .Version = TRowVersion(rowset.GetValue<Schema::Sources::LastHeartbeatStep>(), rowset.GetValue<Schema::Sources::LastHeartbeatTxId>()),
                .Data = rowset.GetValue<Schema::Sources::LastHeartbeatData>(),
            };
        }

        if (!rowset.Next()) {
            return std::nullopt;
        }
    }

    return lastHeartbeats;
}

std::optional<TCdcAggregatorStream::TPartitioningChanges> TCdcAggregatorStreamRepository::LoadPartitioningChanges(NIceDb::TNiceDb& db) const {
    auto rowset = db.Table<Schema::PartitioningChanges>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }

    TCdcAggregatorStream::TPartitioningChanges changes;
    while (!rowset.EndOfSet()) {
        const auto parentDataShardId = rowset.GetValue<Schema::PartitioningChanges::ParentDataShardId>();
        const auto operationId = rowset.GetValue<Schema::PartitioningChanges::OperationId>();
        changes.emplace(std::make_pair(parentDataShardId, operationId), rowset.GetValue<Schema::PartitioningChanges::ChildDataShardIds>());

        if (!rowset.Next()) {
            return std::nullopt;
        }
    }

    return changes;
}

} // namespace NKikimr::NCdcAggregator
