#include "cdc_aggregator_stream_repository.h"

#include <ydb/core/protos/cdc_aggregator.pb.h>

#include <tuple>

namespace NKikimr::NCdcAggregator {

void TCdcAggregatorStreamRepository::InsertCdcAggregatorStream(NIceDb::TNiceDb& db, const TCdcAggregatorStream& stream) const {
    db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
        NIceDb::TUpdate<Schema::State::TrackingConfig>(Converter.SerializeTopic(stream.GetTopic())));

    for (const auto dataShardId : stream.GetSourceDataShardIds()) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }
}

void TCdcAggregatorStreamRepository::ApplyCdcAggregatorStreamPartitioningChange(NIceDb::TNiceDb& db, const TCdcAggregatorStreamPartitioningChange& change, const TCdcAggregatorStreamSourcesDiff& diff) const {
    for (const auto dataShardId : diff.Deleted) {
        db.Table<Schema::Sources>().Key(dataShardId).Delete();
    }

    for (const auto dataShardId : diff.Inserted) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }

    const auto serializedParentDataShardIds = static_cast<TString>(NIceDb::TTypeValue(change.ParentDataShardIds));
    db.Table<Schema::PartitioningChanges>().Key(std::make_tuple(serializedParentDataShardIds, change.OperationId)).Update(
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

    auto sources = LoadSourceDataShardIds(db);
    if (!sources) {
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

    return {true, TCdcAggregatorStream(std::move(*topic), std::move(*sources), std::move(*changes))};
}

std::optional<THashSet<ui64>> TCdcAggregatorStreamRepository::LoadSourceDataShardIds(NIceDb::TNiceDb& db) const {
    auto rowset = db.Table<Schema::Sources>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }

    THashSet<ui64> sources;
    while (!rowset.EndOfSet()) {
        sources.insert(rowset.GetValue<Schema::Sources::DataShardId>());

        if (!rowset.Next()) {
            return std::nullopt;
        }
    }

    return sources;
}

std::optional<TCdcAggregatorStream::TPartitioningChanges> TCdcAggregatorStreamRepository::LoadPartitioningChanges(NIceDb::TNiceDb& db) const {
    auto rowset = db.Table<Schema::PartitioningChanges>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }

    TCdcAggregatorStream::TPartitioningChanges changes;
    while (!rowset.EndOfSet()) {
        auto parentDataShardIds = rowset.GetValue<Schema::PartitioningChanges::ParentDataShardIds>();
        const auto operationId = rowset.GetValue<Schema::PartitioningChanges::OperationId>();
        changes.emplace(std::make_pair(std::move(parentDataShardIds), operationId), rowset.GetValue<Schema::PartitioningChanges::ChildDataShardIds>());

        if (!rowset.Next()) {
            return std::nullopt;
        }
    }

    return changes;
}

} // namespace NKikimr::NCdcAggregator
