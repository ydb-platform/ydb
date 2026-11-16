#pragma once

#include "cdc_aggregator_stream.h"
#include "cdc_aggregator_stream_proto.h"
#include "schema.h"

#include <optional>
#include <utility>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorStreamRepository {
    using Schema = TCdcAggregatorSchema;

public:
    void InsertCdcAggregatorStream(NIceDb::TNiceDb& db, const TCdcAggregatorStream& stream) const;
    void ApplyCdcAggregatorStreamPartitioningChange(NIceDb::TNiceDb& db, const TCdcAggregatorStreamPartitioningChange& change, const TCdcAggregatorStreamSourcesDiff& diff) const;
    std::pair<bool, std::optional<TCdcAggregatorStream>> LoadCdcAggregatorStream(NIceDb::TNiceDb& db) const;

private:
    std::optional<THashSet<ui64>> LoadSourceDataShardIds(NIceDb::TNiceDb& db) const;
    std::optional<TCdcAggregatorStream::TPartitioningChanges> LoadPartitioningChanges(NIceDb::TNiceDb& db) const;

    TCdcAggregatorStreamProtoConverter Converter;
};

} // namespace NKikimr::NCdcAggregator
