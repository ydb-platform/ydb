#pragma once

#include <ydb/core/base/events.h>

namespace NKikimr::NMemory {

struct IMemoryConsumer : public TThrRefBase {
    virtual ui64 GetLimit() const = 0; // TODO: initial limit?
    virtual void SetConsumption(ui64 value) = 0;
};

struct IMemTableMemoryConsumer : public TThrRefBase {
    virtual void SetConsumption(ui64 value) = 0;
};

enum EEvMemory {
    EvMemoryLimit = EventSpaceBegin(TKikimrEvents::ES_MEMORY),
    
    EvMemTableRegister,
    EvMemTableRegistered,
    EvMemTableCompact,
    EvMemTableCompacted,
    EvMemTableUnregister,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_MEMORY), "expected EvEnd < EventSpaceEnd");

struct TEvMemoryLimit : public TEventLocal<TEvMemoryLimit, EvMemoryLimit> {
    ui64 LimitBytes;

    TEvMemoryLimit(ui64 limitBytes)
        : LimitBytes(limitBytes)
    {}
};

struct TEvMemTableRegister : public TEventLocal<TEvMemTableRegister, EvMemTableRegister> {
    const ui32 Table;

    TEvMemTableRegister(ui32 table)
        : Table(table)
    {}
};

struct TEvMemTableRegistered : public TEventLocal<TEvMemTableRegistered, EvMemTableRegistered> {
    const ui32 Table;
    TIntrusivePtr<IMemTableMemoryConsumer> Consumer;

    TEvMemTableRegistered(ui32 table, TIntrusivePtr<IMemTableMemoryConsumer> consumer)
        : Table(table)
        , Consumer(std::move(consumer))
    {}
};

struct TEvMemTableCompact : public TEventLocal<TEvMemTableCompact, EvMemTableCompact> {
    const ui32 Table;
    const ui64 ExpectedSize;

    TEvMemTableCompact(ui32 table, ui64 expectedSize)
        : Table(table)
        , ExpectedSize(expectedSize)
    {}
};

struct TEvMemTableCompacted : public TEventLocal<TEvMemTableCompacted, EvMemTableCompacted> {
    const TIntrusivePtr<IMemTableMemoryConsumer> Consumer;

    TEvMemTableCompacted(TIntrusivePtr<IMemTableMemoryConsumer> consumer)
        : Consumer(consumer)
    {}
};

struct TEvMemTableUnregister : public TEventLocal<TEvMemTableUnregister, EvMemTableUnregister> {
    const ui32 Table;

    TEvMemTableUnregister(ui32 table)
        : Table(table)
    {}
};

inline TActorId MakeMemoryControllerId(ui64 id = 0) {
    char x[12] = { 'm', 'e', 'm', 'c' };
    WriteUnaligned<ui64>((ui64*)(x+4), id);
    return TActorId(0, TStringBuf(x, 12));
}

}
