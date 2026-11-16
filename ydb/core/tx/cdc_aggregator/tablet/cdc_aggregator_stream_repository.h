#pragma once

#include "cdc_aggregator_stream.h"
#include "cdc_aggregator_stream_proto.h"
#include "schema.h"

#include <optional>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorStreamRepository {
    using Schema = TCdcAggregatorSchema;

public:
    void InsertCdcAggregatorStream(NIceDb::TNiceDb& db, const TCdcAggregatorStream& stream) const;
    void ApplyCdcAggregatorStreamSourcesDiff(NIceDb::TNiceDb& db, const TCdcAggregatorStreamSourcesDiff& diff) const;
    bool LoadCdcAggregatorStream(NIceDb::TNiceDb& db, std::optional<TCdcAggregatorStream>& stream) const;

private:
    bool LoadSourceDataShardIds(NIceDb::TNiceDb& db, THashSet<ui64>& sources) const;

    TCdcAggregatorStreamProtoConverter Converter;
};

} // namespace NKikimr::NCdcAggregator
