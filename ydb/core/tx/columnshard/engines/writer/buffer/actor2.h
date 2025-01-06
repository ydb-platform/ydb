#pragma once
#include "events.h"

#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/slice_builder/pack_builder.h>
#include <ydb/core/tx/columnshard/operations/write.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NWritingPortions {

class TAggregationId {
private:
    const ui64 PathId;
    const ui64 SchemaVersion;
    const NEvWrite::EModificationType ModificationType;
    const TString SchemaDescription;

public:
    TAggregationId(const ui64 pathId, const ui64 schemaVersion, const NEvWrite::EModificationType mType,
        const TString& schemaDescription)
        : PathId(pathId)
        , SchemaVersion(schemaVersion)
        , ModificationType(mType)
        , SchemaDescription(schemaDescription) {
    }

    bool operator==(const TAggregationId& item) const {
        return PathId == item.PathId && SchemaVersion == item.SchemaVersion && ModificationType == item.ModificationType &&
               SchemaDescription == item.SchemaDescription;
    }

    operator size_t() const {
        return 0;
    }
};

class TWriteAggregation {
private:
    const ui64 PathId;
    const NEvWrite::EModificationType ModificationType;
    std::vector<TWriteUnit> Units;
    NOlap::TWritingContext Context;
    ui64 SumSize = 0;

public:
    TWriteAggregation(const NOlap::TWritingContext& context, const ui64 pathId, const NEvWrite::EModificationType modificationType)
        : PathId(pathId)
        , ModificationType(modificationType)
        , Context(context) {
    }

    void MergeContext(const NOlap::TWritingContext& newContext) {
        Context.MergeFrom(newContext);
    }

    void AddUnit(TWriteUnit&& unit) {
        SumSize += unit.GetData()->GetSize();
        Units.emplace_back(std::move(unit));
    }

    ui64 GetSumSize() const {
        return SumSize;
    }

    void Flush(const ui64 tabletId);
};

class TActor: public TActorBootstrapped<TActor> {
private:
    THashMap<TAggregationId, TWriteAggregation> Aggregations;
    const ui64 TabletId;
    NActors::TActorId ParentActorId;
    std::optional<TDuration> FlushDuration;
    ui64 SumSize = 0;
    void Flush();

public:
    TActor(ui64 tabletId, const TActorId& parent);
    ~TActor() = default;

    void Handle(TEvAddInsertedDataToBuffer::TPtr& ev);
    void Handle(TEvFlushBuffer::TPtr& ev);
    void Bootstrap();

    STFUNC(StateWait) {
        TLogContextGuard gLogging(
            NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", ParentActorId));
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvAddInsertedDataToBuffer, Handle);
            hFunc(TEvFlushBuffer, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}   // namespace NKikimr::NColumnShard::NWritingPortions
