#pragma once

#include <ydb/core/base/events.h>

namespace NKikimr::NMemory {

enum class EMemoryConsumerKind {
    SharedCache,
    MemTable,
};

struct IMemoryConsumer : public TThrRefBase {
    virtual void SetConsumption(ui64 value) = 0;
};

enum EEvMemory {
    EvConsumerRegister = EventSpaceBegin(TKikimrEvents::ES_MEMORY),
    EvConsumerRegistered,
    EvConsumerLimit,
    
    EvMemTableRegister,
    EvMemTableRegistered,
    EvMemTableCompact,
    EvMemTableCompacted,
    EvMemTableUnregister,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_MEMORY), "expected EvEnd < EventSpaceEnd");

struct TEvConsumerRegister : public TEventLocal<TEvConsumerRegister, EvConsumerRegister> {
    const EMemoryConsumerKind Kind;

    TEvConsumerRegister(EMemoryConsumerKind kind)
        : Kind(kind)
    {}
};

struct TEvConsumerRegistered : public TEventLocal<TEvConsumerRegistered, EvConsumerRegistered> {
    TIntrusivePtr<IMemoryConsumer> Consumer;

    TEvConsumerRegistered(TIntrusivePtr<IMemoryConsumer> consumer)
        : Consumer(std::move(consumer))
    {}
};

struct TEvConsumerLimit : public TEventLocal<TEvConsumerLimit, EvConsumerLimit> {
    ui64 LimitBytes;

    TEvConsumerLimit(ui64 limitBytes)
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
    TIntrusivePtr<IMemoryConsumer> Consumer;

    TEvMemTableRegistered(ui32 table, TIntrusivePtr<IMemoryConsumer> consumer)
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
    const TIntrusivePtr<IMemoryConsumer> MemoryConsumer;

    TEvMemTableCompacted(TIntrusivePtr<IMemoryConsumer> memoryConsumer)
        : MemoryConsumer(std::move(memoryConsumer))
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
