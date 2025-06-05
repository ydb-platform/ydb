#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NSubscriber {
class TEventTablesErased: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(THashSet<TInternalPathId>, PathIds);
    virtual TString DoDebugString() const override;
public:
    TEventTablesErased(const THashSet<TInternalPathId>& pathIds)
        : TBase(EEventType::TablesErased)
        , PathIds(pathIds)
    {

    }
};
}