#pragma once
#include "events.h"

#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard::NWritingPortions {

class TWriteUnit {
private:
    YDB_READONLY_DEF(std::shared_ptr<TWriteData>, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);

public:
    TWriteUnit(const std::shared_ptr<TWriteData>& data, const std::shared_ptr<arrow::RecordBatch>& batch)
        : Data(data)
        , Batch(batch) {
        AFL_VERIFY(Data->GetWritePortions());
        AFL_VERIFY(Batch);
    }
};

class TAggregationId {
private:
    const ui64 PathId;
    const ui64 SchemaVersion;
    const NEvWrite::EModificationType ModificationType;
    const EOperationBehaviour Behaviour;
    const TString SchemaDescription;

public:
    TAggregationId(const ui64 pathId, const ui64 schemaVersion, const NEvWrite::EModificationType mType, const EOperationBehaviour behaviour,
        const TString& schemaDescription)
        : PathId(pathId)
        , SchemaVersion(schemaVersion)
        , ModificationType(mType)
        , Behaviour(behaviour)
        , SchemaDescription(schemaDescription) {
    }

    bool operator==(const TAggregationId& item) const {
        return PathId == item.PathId && SchemaVersion == item.SchemaVersion && ModificationType == item.ModificationType &&
               Behaviour == item.Behaviour && SchemaDescription == item.SchemaDescription;
    }

    operator size_t() const {
        return 0;
    }
};

class TWriteAggregation {
private:
    YDB_READONLY(ui64, SumSize, 0);
    const ui64 PathId;
    const EModificationType ModificationType;
    std::vector<TWriteUnit> Units;
    TWritingContext Context;

public:
    TWriteAggregation(const TWritingContext& context, const ui64 pathId, const EModificationType modificationType)
        : PathId(pathId)
        , ModificationType(modificationType)
        , Context(context) {
    }

    void MergeContext(const TWritingContext& newContext) {
        Context.MergeFrom(newContext);
    }

    void AddUnit(TWriteUnit&& unit) {
        SumSize += unit.GetData()->GetSize();
        Units.emplace_back(std::move(unit));
    }

    void Flush() {
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<NOlap::TBuildPackSlicesTask>(std::move(Units), Context, PathId, ModificationType);
        NConveyor::TInsertServiceOperator::AsyncTaskToExecute(task);
        SumSize = 0;
        Units.clear();
    }
};

class TActor: public TActorBootstrapped<TActor> {
private:
    THashMap<TAggregationId, TWriteAggregation> Aggregations;
    const ui64 TabletId;
    NActors::TActorId ParentActorId;
    std::optional<TDuration> FlushDuration;
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
