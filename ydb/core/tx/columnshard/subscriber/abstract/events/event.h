#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/generic/string.h>

namespace NKikimr::NColumnShard::NSubscriber {

enum class EEventType {
    Undefined,
    TablesErased,
    AppendCompleted
};

class ISubscriptionEvent {
private:
    YDB_READONLY(EEventType, Type, EEventType::Undefined);
    virtual TString DoDebugString() const {
        return "";
    }
public:
    virtual ~ISubscriptionEvent() = default;

    ISubscriptionEvent(const EEventType type)
        : Type(type)
    {

    }

    TString DebugString() const {
        return DoDebugString();
    }
};

}
