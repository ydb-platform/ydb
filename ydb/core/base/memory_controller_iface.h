#pragma once

#include <ydb/core/base/events.h>

namespace NKikimr::NMemory {

struct IMemoryConsumer : public TThrRefBase {
    virtual ui64 GetLimit() const = 0; // TODO: initial limit?
    virtual void SetConsumption(ui64 value) = 0;
};

enum EEvMemory {
    EvMemoryLimit = EventSpaceBegin(TKikimrEvents::ES_MEMORY),

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_MEMORY), "expected EvEnd < EventSpaceEnd");

struct TEvMemoryLimit : public TEventLocal<TEvMemoryLimit, EvMemoryLimit> {
    ui64 LimitBytes;

    TEvMemoryLimit(ui64 limitBytes)
        : LimitBytes(limitBytes)
    {}
};

}
