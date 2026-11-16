#include "cdc_aggregator_stream_repository.h"

namespace NKikimr::NCdcAggregator {

void TCdcAggregatorStreamRepository::InsertCdcAggregatorStream(NIceDb::TNiceDb& db, const TCdcAggregatorStream& stream) const {
    db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
        NIceDb::TUpdate<Schema::State::TrackingConfig>(Converter.SerializeTopic(stream.GetTopic())));

    for (const auto dataShardId : stream.GetSourceDataShardIds()) {
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

std::pair<bool, std::optional<TCdcAggregatorStream>> TCdcAggregatorStreamRepository::LoadCdcAggregatorStream(NIceDb::TNiceDb& db) const {
    auto state = db.Table<Schema::State>().Key(Schema::State::Id::Value).Select();
    if (!state.IsReady()) {
        return {false, std::nullopt};
    }
    if (!state.IsValid() || !state.HaveValue<Schema::State::TrackingConfig>()) {
        return {true, std::nullopt};
    }

    THashSet<ui64> sources;
    if (!LoadSourceDataShardIds(db, sources)) {
        return {false, std::nullopt};
    }

    NKikimrCdcAggregator::TCdcAggregatorStreamTopic proto;
    Y_ENSURE(proto.ParseFromString(state.GetValue<Schema::State::TrackingConfig>()), "Malformed tracking config");
    auto [topic, error] = Converter.ParseTopic(proto);
    Y_ENSURE(topic, "Invalid tracking config: " << error);

    return {true, TCdcAggregatorStream(std::move(*topic), std::move(sources))};
}

bool TCdcAggregatorStreamRepository::LoadSourceDataShardIds(NIceDb::TNiceDb& db, THashSet<ui64>& sources) const {
    auto rowset = db.Table<Schema::Sources>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        sources.insert(rowset.GetValue<Schema::Sources::DataShardId>());

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

} // namespace NKikimr::NCdcAggregator
