#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>
#include <ydb/core/tx/columnshard/engines/defs.h>

namespace NKikimr::NColumnShard::NSubscriber {

class TEventWritesCompleted: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(NOlap::TWriteId, WriteId);
public:
    TEventWritesCompleted(const NOlap::TWriteId writeId)
        : TBase(EEventType::WritesCompleted)
        , WriteId(writeId)
    {
    }
    TString DoDebugString() const override {
        return "write_id=" + std::to_string(static_cast<ui64>(WriteId));
    }

};

} //namespace NKikimr::NColumnShard::NSubscriber