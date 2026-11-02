#pragma once

#include <ydb/core/base/events.h>

#include <atomic>
#include <limits>

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

    QueryExecution,
};

struct TConsumerReport {
    ui64 Used = 0;
    ui64 Demand = 0; // total desired footprint, invariant: Demand >= Used
    ui64 Reclaimable = 0; // releasable asynchronously on request, invariant: Reclaimable <= Used
};

struct IMemoryConsumer : public TThrRefBase {
    virtual void SetReport(TConsumerReport report) = 0;

    // Configured maximum for automatic grants; max ui64 means unbounded.
    void SetLimitCeiling(ui64 value) {
        LimitCeiling.store(value);
    }

    ui64 GetLimitCeiling() const {
        return LimitCeiling.load();
    }

    void SetConsumption(ui64 value) {
        SetReport({.Used = value, .Demand = value, .Reclaimable = 0});
    }

private:
    std::atomic<ui64> LimitCeiling{std::numeric_limits<ui64>::max()};
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
    // Permission before the first grant, in TEvConsumerLimit units; zero forbids growth.
    // An unbounded startup cap is represented by the maximum ui64 value.
    const ui64 InitialLimitBytes;
    const ui64 LimitCeiling;

    TEvConsumerRegister(EMemoryConsumerKind kind, ui64 initialLimitBytes = 0,
        ui64 limitCeiling = std::numeric_limits<ui64>::max())
        : Kind(kind)
        , InitialLimitBytes(initialLimitBytes)
        , LimitCeiling(limitCeiling)
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
        : LimitBytes(limitBytes) {
    }
};

// Replaces the automatic share with a total limit in TEvConsumerLimit units.
// Zero keeps this registration at zero; re-register to restore automatic allocation.
// Increases wait for the full target. The caller must respect its configured ceiling.
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
