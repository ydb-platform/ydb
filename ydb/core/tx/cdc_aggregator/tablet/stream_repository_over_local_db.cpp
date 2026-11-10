#include "stream_repository_over_local_db.h"
#include "protocol.h"
#include "schema.h"

namespace NKikimr::NCdcAggregator {

using Schema = TCdcAggregatorSchema;

TStreamRepositoryOverLocalDb::TStreamRepositoryOverLocalDb(NTable::TDatabase& database)
    : Db(database)
{
}

bool TStreamRepositoryOverLocalDb::Load(TStream& stream) {
    auto configRowset = Db.Table<Schema::Config>().Key(Schema::SingleRowId).Select();
    auto sourcesRowset = Db.Table<Schema::Sources>().Select();
    auto barrierRowset = Db.Table<Schema::ReadyBarrier>().Key(Schema::SingleRowId).Select();
    auto replacementsRowset = Db.Table<Schema::SourceReplacements>().Select();
    if (!configRowset.IsReady() || !sourcesRowset.IsReady() || !barrierRowset.IsReady() || !replacementsRowset.IsReady()) {
        return false;
    }

    if (!configRowset.IsValid()) {
        Y_ENSURE(sourcesRowset.EndOfSet() && !barrierRowset.IsValid() && replacementsRowset.EndOfSet(),
            "Stream state is stored without a configuration");
        stream = TStream();
        return true;
    }

    NKikimrCdcAggregator::TStreamConfig configProto;
    Y_ENSURE(configProto.ParseFromString(configRowset.GetValue<Schema::Config::StreamConfig>()));
    const auto phase = configRowset.GetValue<Schema::Config::Phase>();

    THashMap<TSourceId, TSourceProgress> sources;
    while (!sourcesRowset.EndOfSet()) {
        auto& progress = sources[sourcesRowset.GetValue<Schema::Sources::SourceId>()];
        if (sourcesRowset.HaveValue<Schema::Sources::Step>()) {
            progress = TBarrier{
                .Version = TRowVersion(
                    sourcesRowset.GetValue<Schema::Sources::Step>(),
                    sourcesRowset.GetValue<Schema::Sources::TxId>()),
                .Payload = sourcesRowset.GetValue<Schema::Sources::Payload>(),
            };
        }
        if (!sourcesRowset.Next()) {
            return false;
        }
    }

    std::optional<TBarrier> readyBarrier;
    if (barrierRowset.IsValid()) {
        readyBarrier = TBarrier{
            .Version = TRowVersion(
                barrierRowset.GetValue<Schema::ReadyBarrier::Step>(),
                barrierRowset.GetValue<Schema::ReadyBarrier::TxId>()),
            .Payload = barrierRowset.GetValue<Schema::ReadyBarrier::Payload>(),
        };
    }

    // Rows are ordered by key, so children come sorted.
    THashMap<TSourceId, TSourceReplacement> replacements;
    while (!replacementsRowset.EndOfSet()) {
        const auto parent = replacementsRowset.GetValue<Schema::SourceReplacements::ParentId>();
        auto& replacement = replacements[parent];
        replacement.Parent = parent;
        replacement.Children.push_back(replacementsRowset.GetValue<Schema::SourceReplacements::ChildId>());
        replacement.OperationId = replacementsRowset.GetValue<Schema::SourceReplacements::OperationId>();
        if (!replacementsRowset.Next()) {
            return false;
        }
    }

    stream = TStream::Restore(ParseStreamConfig(configProto), phase,
        std::move(sources), std::move(readyBarrier), std::move(replacements));
    return true;
}

void TStreamRepositoryOverLocalDb::Save(const TStream& stream, const TStreamChanges& changes) {
    if (changes.Config) {
        NKikimrCdcAggregator::TStreamConfig configProto;
        FillStreamConfig(stream.GetConfig(), configProto);
        Db.Table<Schema::Config>().Key(Schema::SingleRowId).Update(
            NIceDb::TUpdate<Schema::Config::StreamConfig>(configProto.SerializeAsString()),
            NIceDb::TUpdate<Schema::Config::Phase>(stream.GetPhase()));
    }

    for (const auto source : changes.UpsertedSources) {
        const auto* progress = stream.FindSourceProgress(source);
        Y_ENSURE(progress, "Upserted source " << source << " is not in the stream");
        if (*progress) {
            const auto& barrier = **progress;
            Db.Table<Schema::Sources>().Key(source).Update(
                NIceDb::TUpdate<Schema::Sources::Step>(barrier.Version.Step),
                NIceDb::TUpdate<Schema::Sources::TxId>(barrier.Version.TxId),
                NIceDb::TUpdate<Schema::Sources::Payload>(barrier.Payload));
        } else {
            Db.Table<Schema::Sources>().Key(source).Update();
        }
    }

    for (const auto source : changes.ErasedSources) {
        Db.Table<Schema::Sources>().Key(source).Delete();
    }

    if (changes.ReadyBarrier) {
        const auto& barrier = stream.GetReadyBarrier();
        Y_ENSURE(barrier, "Changed ready barrier is empty");
        Db.Table<Schema::ReadyBarrier>().Key(Schema::SingleRowId).Update(
            NIceDb::TUpdate<Schema::ReadyBarrier::Step>(barrier->Version.Step),
            NIceDb::TUpdate<Schema::ReadyBarrier::TxId>(barrier->Version.TxId),
            NIceDb::TUpdate<Schema::ReadyBarrier::Payload>(barrier->Payload));
    }

    for (const auto parent : changes.AddedReplacements) {
        const auto& replacement = stream.GetReplacements().at(parent);
        for (const auto child : replacement.Children) {
            Db.Table<Schema::SourceReplacements>().Key(parent, child).Update(
                NIceDb::TUpdate<Schema::SourceReplacements::OperationId>(replacement.OperationId));
        }
    }
}

} // namespace NKikimr::NCdcAggregator
