#pragma once
#include "dependencies.h"
#include "interaction.h"

#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap::NTxInteractions {

class ITxEvent {
public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<ITxEvent, TString>;
    using TProto = NKikimrColumnShardTxProto::TEvent;

protected:
    virtual void DoAddToInteraction(const ui64 txId, TInteractionsContext& context) const = 0;
    virtual void DoRemoveFromInteraction(const ui64 txId, TInteractionsContext& context) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardTxProto::TEvent& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardTxProto::TEvent& proto) const = 0;

public:
    ITxEvent() = default;
    virtual ~ITxEvent() = default;

    virtual TString GetClassName() const = 0;

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }

    void AddToInteraction(const ui64 txId, TInteractionsContext& context) const {
        return DoAddToInteraction(txId, context);
    }

    void RemoveFromInteraction(const ui64 txId, TInteractionsContext& context) const {
        return DoRemoveFromInteraction(txId, context);
    }
};

class TTxEventContainer: public NBackgroundTasks::TInterfaceProtoContainer<ITxEvent> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ITxEvent>;
    YDB_READONLY(ui64, TxId, 0);
    YDB_READONLY(ui64, Generation, 0);
    static inline TAtomicCounter IdsCounter = 0;
    ui64 GenerationInternalId = 0;

public:
    void AddToInteraction(TInteractionsContext& context) const {
        return GetObjectVerified().AddToInteraction(TxId, context);
    }

    void RemoveFromInteraction(TInteractionsContext& context) const {
        return GetObjectVerified().RemoveFromInteraction(TxId, context);
    }

    ui64 GetGenerationInternalId() const {
        return GenerationInternalId;
    }

    TTxEventContainer(const ui64 txId, const ui64 generation, const std::shared_ptr<ITxEvent>& txEvent)
        : TBase(txEvent)
        , TxId(txId)
        , Generation(generation)
        , GenerationInternalId(IdsCounter.Inc()) {
    }

    TTxEventContainer(const ui64 txId, const ui64 generation, const ui64 id, const std::shared_ptr<ITxEvent>& txEvent)
        : TBase(txEvent)
        , TxId(txId)
        , Generation(generation)
        , GenerationInternalId(id) {
    }

    TTxEventContainer(const ui64 txId, const ui64 generation, const ui64 id)
        : TxId(txId)
        , Generation(generation)
        , GenerationInternalId(id) {
    }

    bool operator<(const TTxEventContainer& item) const {
        return std::tie(Generation, GenerationInternalId) < std::tie(item.Generation, item.GenerationInternalId);
    }
};

class ITxEventWriter {
protected:
    virtual TTxConflicts DoCheckInteraction(const ui64 selfTxId, TInteractionsContext& context) const = 0;
    virtual std::shared_ptr<ITxEvent> DoBuildEvent() = 0;

public:
    ITxEventWriter() = default;

    TTxConflicts CheckInteraction(const ui64 selfTxId, TInteractionsContext& context) const {
        return DoCheckInteraction(selfTxId, context);
    }

    std::shared_ptr<ITxEvent> BuildEvent() {
        return DoBuildEvent();
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
