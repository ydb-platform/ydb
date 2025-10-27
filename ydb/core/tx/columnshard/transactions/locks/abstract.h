#pragma once
#include "dependencies.h"
#include "interaction.h"

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimrColumnShardTxProto {
class TEvent;
}

namespace NKikimr::NOlap::NTxInteractions {

class ITxEvent {
public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<ITxEvent, TString>;
    using TProto = NKikimrColumnShardTxProto::TEvent;

protected:
    virtual void DoAddToInteraction(const ui64 lockId, TInteractionsContext& context) const = 0;
    virtual void DoRemoveFromInteraction(const ui64 lockId, TInteractionsContext& context) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardTxProto::TEvent& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardTxProto::TEvent& proto) const = 0;

public:
    ITxEvent() = default;
    virtual ~ITxEvent() = default;

    virtual TString GetClassName() const = 0;

    virtual NColumnShard::TUnifiedPathId GetPathId() const = 0;

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }

    void AddToInteraction(const ui64 lockId, TInteractionsContext& context) const {
        return DoAddToInteraction(lockId, context);
    }

    void RemoveFromInteraction(const ui64 lockId, TInteractionsContext& context) const {
        return DoRemoveFromInteraction(lockId, context);
    }
};

class TTxEventContainer: public NBackgroundTasks::TInterfaceProtoContainer<ITxEvent> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ITxEvent>;
    YDB_READONLY(ui64, LockId, 0);

public:
    void AddToInteraction(TInteractionsContext& context) const {
        return GetObjectVerified().AddToInteraction(LockId, context);
    }

    void RemoveFromInteraction(TInteractionsContext& context) const {
        return GetObjectVerified().RemoveFromInteraction(LockId, context);
    }

    TTxEventContainer(const ui64 lockId, const std::shared_ptr<ITxEvent>& txEvent)
        : TBase(txEvent)
        , LockId(lockId) {
    }

    TTxEventContainer(const ui64 lockId)
        : LockId(lockId) {
    }

    bool operator<(const TTxEventContainer& item) const {
        return LockId < item.LockId;
    }
};

class ITxEventWriter {
protected:
    virtual bool DoCheckInteraction(
        const ui64 selfLockId, TInteractionsContext& context, TTxConflicts& conflicts, TTxConflicts& notifications) const = 0;
    virtual std::shared_ptr<ITxEvent> DoBuildEvent() = 0;

public:
    ITxEventWriter() = default;
    virtual ~ITxEventWriter() = default;

    bool CheckInteraction(const ui64 selfLockId, TInteractionsContext& context, TTxConflicts& conflicts, TTxConflicts& notifications) const {
        TTxConflicts conflictsResult;
        TTxConflicts notificationsResult;
        const bool result = DoCheckInteraction(selfLockId, context, conflictsResult, notificationsResult);
        std::swap(conflictsResult, conflicts);
        std::swap(notificationsResult, notifications);
        return result;
    }

    std::shared_ptr<ITxEvent> BuildEvent() {
        return DoBuildEvent();
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
