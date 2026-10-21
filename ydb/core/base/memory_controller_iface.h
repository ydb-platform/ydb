#pragma once

#include <ydb/core/base/events.h>

namespace NKikimr::NMemory {

enum class EMemoryConsumerKind {
    SharedCache,
    MemTable,

    ColumnTablesScanGroupedMemory,
    ColumnTablesCompGroupedMemory,
    ColumnTablesBlobCache,
    ColumnTablesDataAccessorCache,
    ColumnTablesColumnDataCache,
    ColumnTablesDeduplicationGroupedMemory,
    ColumnTablesPortionsMetaDataCache,
};

struct TConsumerReport {
    ui64 Used = 0;
    ui64 Demand = 0; // total desired footprint, invariant: Demand >= Used
    ui64 Reclaimable = 0; // releasable asynchronously on request, invariant: Reclaimable <= Used
};

struct IMemoryConsumer : public TThrRefBase {
    virtual void SetReport(TConsumerReport report) = 0;

    void SetConsumption(ui64 value) {
        SetReport({.Used = value, .Demand = value, .Reclaimable = 0});
    }
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

    EvConsumerUnregister,
    EvConsumerDemand,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_MEMORY), "expected EvEnd < EventSpaceEnd");

struct TEvConsumerRegister : public TEventLocal<TEvConsumerRegister, EvConsumerRegister> {
    const EMemoryConsumerKind Kind;
    // Capacity the consumer may use before it receives its first MC limit.
    // MC must reserve it even when current usage is zero. Zero means the
    // consumer cannot grow until MC grants a limit; it does not mean unknown.
    const ui64 InitialLimitBytes;

    TEvConsumerRegister(EMemoryConsumerKind kind, ui64 initialLimitBytes = 0)
        : Kind(kind)
        , InitialLimitBytes(initialLimitBytes)
    {}
};

// Sent after the registrant has stopped using its grant and serving this kind.
struct TEvConsumerUnregister : public TEventLocal<TEvConsumerUnregister, EvConsumerUnregister> {
    const EMemoryConsumerKind Kind;

    TEvConsumerUnregister(EMemoryConsumerKind kind)
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
    {
    }
};

// A registered consumer replaces its target for subsequent allocation rounds.
// Desired is a total limit, in the same units as TEvConsumerLimit::LimitBytes.
// The configured share is used until the first request; zero is an explicit
// release. An increase waits for the complete target to fit, then MC sends
// TEvConsumerLimit. The consumer must keep using its previous grant meanwhile.
struct TEvConsumerDemand : public TEventLocal<TEvConsumerDemand, EvConsumerDemand> {
    const EMemoryConsumerKind Kind;
    const ui64 Desired;

    TEvConsumerDemand(EMemoryConsumerKind kind, ui64 desired)
        : Kind(kind)
        , Desired(desired)
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
