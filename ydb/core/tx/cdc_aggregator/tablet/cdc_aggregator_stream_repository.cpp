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

bool TCdcAggregatorStreamRepository::LoadCdcAggregatorStream(NIceDb::TNiceDb& db, std::optional<TCdcAggregatorStream>& stream) const {
    auto state = db.Table<Schema::State>().Key(Schema::State::Id::Value).Select();
    if (!state.IsReady()) {
        return false;
    }
    if (!state.IsValid()) {
        stream.reset();
        return true;
    }

    THashSet<ui64> sources;
    if (!LoadSourceDataShardIds(db, sources)) {
        return false;
    }

    NKikimrCdcAggregator::TTrackingConfig proto;
    Y_ENSURE(proto.ParseFromString(state.GetValue<Schema::State::TrackingConfig>()), "Malformed tracking config");
    auto [topic, error] = Converter.ParseTopic(proto);
    Y_ENSURE(topic, "Invalid tracking config: " << error);

    stream.emplace(std::move(*topic), std::move(sources));
    return true;
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
