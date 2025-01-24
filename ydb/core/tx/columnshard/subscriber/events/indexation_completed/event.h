#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>
#include <ydb/core/tx/columnshard/engines/defs.h>

namespace NKikimr::NColumnShard::NSubscriber {

class TEventIndexationCompleted: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(ui64, PathId);
public:
    TEventIndexationCompleted(const ui64 pathId)
        : TBase(EEventType::IndexationCompleted)
        , PathId(pathId)
    {
    }
    TString DoDebugString() const override {
        return "path_id=" + std::to_string(static_cast<ui64>(PathId));
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber
