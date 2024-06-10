#pragma once
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard::NSubscriber {

enum class EEventType {
    Undefined,
    TablesErased
};

class ISubscriptionEvent {
private:
    YDB_READONLY(EEventType, Type, EEventType::Undefined);
public:
    ISubscriptionEvent(const EEventType type)
        : Type(type)
    {

    }
};

}