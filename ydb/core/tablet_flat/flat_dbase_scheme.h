#pragma once

#include "flat_table_column.h"
#include "flat_page_iface.h"
#include "util_basics.h"

#include <ydb/core/base/localdb.h>
#include <ydb/core/protos/scheme_log.pb.h>

#include <util/generic/map.h>
#include <util/generic/hash_set.h>
#include <util/generic/list.h>

namespace NKikimr {
namespace NTable {

using namespace NTabletFlatScheme;

using NKikimrSchemeOp::ECompactionStrategy;

using TCompactionPolicy = NLocalDb::TCompactionPolicy;

class TScheme {
public:
    using ECache = NPage::ECache;

    enum EDefault {
        DefaultRoom = 0,
        DefaultChannel = 1,
    };

    struct TRoom /* Storage unit settings (page collection) */ {
        TRoom() = default;

        TRoom(ui32 channel)
            : Main(channel)
        {

        }

        ui8 Main = DefaultChannel;      /* Primary channel for page collection  */
        TVector<ui8> Blobs = {DefaultChannel};     /* Channels for external blobs   */
        ui8 Outer = DefaultChannel;     /* Channel for outer values pack*/
    };

    struct TFamily /* group of columns configuration */ {
        using ECodec = NPage::ECodec;

        ui32 Room = DefaultRoom;
        ECache Cache = ECache::None;    /* How to cache data pages      */
        ECodec Codec = ECodec::Plain;   /* How to encode data pages     */
        ui32 Small = Max<ui32>();       /* When pack values to outer blobs  */
        ui32 Large = Max<ui32>();       /* When keep values in single blobs */
    };

    using TColumn = NTable::TColumn;

    struct TTableSchema {
        using TColumns = THashMap<ui32, TColumn>;
        using TColumnNames = THashMap<TString, ui32>;

        TColumns Columns;
        TColumnNames ColumnNames;
        TVector<ui32> KeyColumns; // key columns sorted by order
    };

    struct TTableInfo : public TTableSchema {
        TTableInfo(TString name, ui32 id)
            : Id(id)
            , Name(std::move(name))
            , CompactionPolicy(NLocalDb::CreateDefaultTablePolicy())
        {
            Families[TColumn::LeaderFamily];
            Rooms[DefaultRoom];
        }

        ui32 Id;
        TString Name;
        THashMap<ui32, TRoom> Rooms;
        THashMap<ui32, TFamily> Families;

        TIntrusiveConstPtr<TCompactionPolicy> CompactionPolicy;
        bool ColdBorrow = false;
        bool ByKeyFilter = false;
        bool EraseCacheEnabled = false;
        ui32 EraseCacheMinRows = 0; // 0 means use default
        ui32 EraseCacheMaxBytes = 0; // 0 means use default

        // When true this table has an in-memory caching enabled that has not been processed yet
        mutable bool PendingCacheEnable = false;
    };

    struct TRedo {
        /* Do not put cell values below this byte limit to external blobs
            on writing redo log update entries (annex to log). By default
            this limit is taken from Large field of family settings. Thus

                effective = Max(Annex, TFamily::Large)

            However cell value may be converted to external blob eventually
            on compaction if Annex field overrides actual Large setting for
            cell. This feature is required for testing and for ability to
            turn off annex at all putting Max<ui32>() to Annex setting.
         */

        ui32 Annex = 512;   /* some reasonably low default value */
    };

    struct TExecutorInfo {
        ui64 CacheSize = 384 * 1024; // (DEPRECATED)
        bool AllowLogBatching = false;
        bool LogFastTactic = true;
        TDuration LogFlushPeriod = TDuration::MicroSeconds(500);
        ui32 LimitInFlyTx = 0;
        TString ResourceProfile = "default";
        ECompactionStrategy DefaultCompactionStrategy = NKikimrSchemeOp::CompactionStrategyGenerational;
    };

    const TTableInfo* GetTableInfo(ui32 id) const { return const_cast<TScheme*>(this)->GetTableInfo(id); }
    const TColumn* GetColumnInfo(ui32 table, ui32 id) const { return const_cast<TScheme*>(this)->GetColumnInfo(const_cast<TScheme*>(this)->GetTableInfo(table), id); }

    TAutoPtr<TSchemeChanges> GetSnapshot() const;

    inline TTableInfo* GetTableInfo(ui32 id) {
        return Tables.FindPtr(id);
    }

    inline TColumn* GetColumnInfo(TTableInfo* ptable, ui32 id) {
        return ptable ? ptable->Columns.FindPtr(id) : nullptr;
    }

    inline const TColumn* GetColumnInfo(const TTableInfo* ptable, ui32 id) const {
        return ptable ? ptable->Columns.FindPtr(id) : nullptr;
    }

    bool IsEmpty() const {
        return Tables.empty();
    }

    const TRoom* DefaultRoomFor(ui32 id) const noexcept
    {
        if (auto *table = GetTableInfo(id))
            return table->Rooms.FindPtr(DefaultRoom);

        return nullptr;
    }

    const TFamily* DefaultFamilyFor(ui32 id) const noexcept
    {
        if (auto *table = GetTableInfo(id))
            return table->Families.FindPtr(TColumn::LeaderFamily);

        return nullptr;
    }

    ECompactionStrategy CompactionStrategyFor(ui32 id) const noexcept
    {
        if (auto *table = GetTableInfo(id)) {
            auto strategy = table->CompactionPolicy->CompactionStrategy;
            if (strategy != NKikimrSchemeOp::CompactionStrategyUnset) {
                if (strategy == NKikimrSchemeOp::CompactionStrategySharded) {
                    // Sharded strategy doesn't exist anymore
                    // Use the safe generational strategy instead
                    strategy = NKikimrSchemeOp::CompactionStrategyGenerational;
                }
                return strategy;
            }
        }

        return Executor.DefaultCompactionStrategy;
    }

    THashMap<ui32, TTableInfo> Tables;
    THashMap<TString, ui32> TableNames;
    TExecutorInfo Executor;
    TRedo Redo;
};

/**
 * This structure holds the rollback state for database schema, which allows
 * schema to be rolled back to initial state when transaction decides not to
 * commit. As this almost never happens it doesn't need to be very efficient.
 */
struct TSchemeRollbackState {
    // This hash map has key for each modified table, where value either holds
    // previous table schema, or empty if table didn't exist.
    THashMap<ui32, std::optional<TScheme::TTableInfo>> Tables;
    // Previous executor settings if modified
    std::optional<TScheme::TExecutorInfo> Executor;
    // Previous redo settings if modified
    std::optional<TScheme::TRedo> Redo;
};

/**
 * Sink is used to inspect and apply alter records
 */
class IAlterSink {
public:
    /**
     * Sink attempts to apply the given alter record. When the return value
     * is true the record is deemed useful, otherwise it is discarded.
     */
    virtual bool ApplyAlterRecord(const TAlterRecord& record) = 0;
};

// scheme delta
class TAlter {
public:
    using ECodec = NPage::ECodec;
    using ECache = NPage::ECache;

    TAlter(IAlterSink* sink = nullptr)
        : Sink(sink)
    { }

    explicit operator bool() const noexcept;

    const TSchemeChanges& operator*() const noexcept
    {
        return Log;
    }

    TAlter& Merge(const TSchemeChanges &delta);
    TAlter& AddTable(const TString& name, ui32 id);
    TAlter& DropTable(ui32 id);
    TAlter& AddColumn(ui32 table, const TString& name, ui32 id, ui32 type, bool notNull, TCell null = { });
    TAlter& AddColumnWithTypeInfo(ui32 table, const TString& name, ui32 id, ui32 type, const std::optional<NKikimrProto::TTypeInfo>& typeInfoProto, bool notNull, TCell null = { });
    TAlter& DropColumn(ui32 table, ui32 id);
    TAlter& AddColumnToFamily(ui32 table, ui32 column, ui32 family);
    TAlter& AddFamily(ui32 table, ui32 family, ui32 room);
    TAlter& AddColumnToKey(ui32 table, ui32 column);
    TAlter& SetFamily(ui32 table, ui32 family, ECache cache, ECodec codec);
    TAlter& SetFamilyBlobs(ui32 table, ui32 family, ui32 small, ui32 large);
    TAlter& SetRoom(ui32 table, ui32 room, ui32 main, const TSet<ui32>& blobs, ui32 outer);
    TAlter& SetRedo(ui32 annex);
    TAlter& SetExecutorCacheSize(ui64 cacheSize);
    TAlter& SetExecutorFastLogPolicy(bool allow);
    TAlter& SetExecutorAllowLogBatching(bool allow);
    TAlter& SetExecutorLogFlushPeriod(TDuration flushPeriod);
    TAlter& SetExecutorLimitInFlyTx(ui32 limitTxInFly);
    TAlter& SetExecutorResourceProfile(const TString &name);
    TAlter& SetCompactionPolicy(ui32 tableId, const TCompactionPolicy& newPolicy);
    TAlter& SetByKeyFilter(ui32 tableId, bool enabled);
    TAlter& SetColdBorrow(ui32 tableId, bool enabled);
    TAlter& SetEraseCache(ui32 tableId, bool enabled, ui32 minRows, ui32 maxBytes);
    TAlter& SetRewrite();

    TAutoPtr<TSchemeChanges> Flush();

private:
    TAlter& ApplyLastRecord();

protected:
    IAlterSink* Sink;
    TSchemeChanges Log;
};

}}
