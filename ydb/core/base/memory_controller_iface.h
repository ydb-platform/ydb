#pragma once

#include "defs.h"
#include <ydb/core/base/events.h>

namespace NKikimr::NMemory {

enum class EConsumerKind {
    SharedCache,
    MemTable,
};

struct IMemoryConsumer : public TThrRefBase {
    virtual ui64 GetLimit() const = 0;
    virtual void SetConsumption(ui64 value) = 0;
};

struct IMemoryConsumers : public TThrRefBase {
    virtual TIntrusivePtr<IMemoryConsumer> Register(EConsumerKind consumer) = 0;
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
