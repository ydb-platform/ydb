#pragma once

#include <ydb/core/tx/cdc_aggregator/stream/stream_repository.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator::NTest {

inline const TPathId StreamPathId(72075186233409546, 42);

inline TStreamConfig MakeStreamConfig(TVector<TSourceId> sources, ui64 version = 1) {
    return TStreamConfig{
        .StreamPathId = StreamPathId,
        .SchemeShardId = 72075186233409546,
        .Version = version,
        .Sources = std::move(sources),
        .Partitions = {{.PartitionId = 0, .TabletId = 72075186224037900}, {.PartitionId = 1, .TabletId = 72075186224037901}},
    };
}

inline TBarrier MakeBarrier(ui64 step, ui64 txId = 0) {
    return TBarrier{
        .Version = TRowVersion(step, txId),
        .Payload = TStringBuilder() << "{\"resolved\":[" << step << "," << txId << "]}",
    };
}

inline TStream MakeConfiguredStream(TVector<TSourceId> sources) {
    TStream stream;
    TStreamChanges changes;
    Y_ENSURE(stream.Configure(MakeStreamConfig(std::move(sources)), changes) == EConfigureOutcome::Configured);
    return stream;
}

// Remembers every save instead of writing it anywhere.
class TRecordingRepository : public IStreamRepository {
public:
    void Save(const TStream&, const TStreamChanges& changes) override {
        Saved.push_back(changes);
    }

    TVector<TStreamChanges> Saved;
};

} // namespace NKikimr::NCdcAggregator::NTest
