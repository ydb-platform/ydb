#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NSubscriber {
class TEventTablesErased: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(THashSet<ui64>, PathIds);
    virtual TString DoDebugString() const override;
public:
    TEventTablesErased(const THashSet<ui64>& pathIds)
        : TBase(EEventType::TablesErased)
        , PathIds(pathIds)
    {

    }
};
}