#pragma once
#include "events.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NColumnShard::NWriting {

class TActor: public TActorBootstrapped<TActor> {
private:
    std::vector<std::shared_ptr<NOlap::TWriteAggregation>> Aggregations;
    const ui64 TabletId;
    NActors::TActorId ParentActorId;
    TDuration FlushDuration = TDuration::Zero();
    ui64 SumSize = 0;
    void Flush();
public:
    TActor(ui64 tabletId, const TActorId& parent);
    ~TActor() = default;

    void Handle(TEvAddInsertedDataToBuffer::TPtr& ev);
    void Handle(TEvFlushBuffer::TPtr& ev);
    void Bootstrap();

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", ParentActorId));
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvAddInsertedDataToBuffer, Handle);
            hFunc(TEvFlushBuffer, Handle);
            default:
                AFL_VERIFY(false);
        }
    }
};

}
