#pragma once

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>

#include <util/generic/ptr.h>
#include <util/generic/hash.h>

namespace NKikimr {

namespace NTabletFlatExecutor {
class TTransactionContext;
}

namespace NDataShard {

//
struct TUserTable : public TThrRefBase {
    using TPtr = TIntrusivePtr<TUserTable>;
    using TCPtr = TIntrusiveConstPtr<TUserTable>;
    using TTableInfos = THashMap<ui64, TUserTable::TCPtr>;

    struct TUserFamily {
        using ECodec = NTable::NPage::ECodec;
        using ECache = NTable::NPage::ECache;

        TUserFamily(const NKikimrSchemeOp::TFamilyDescription& family)
            : ColumnCodec(family.GetColumnCodec())
            , ColumnCache(family.GetColumnCache())
            , OuterThreshold(SaveGetThreshold(family.GetStorageConfig().GetDataThreshold()))
            , ExternalThreshold(SaveGetThreshold(family.GetStorageConfig().GetExternalThreshold()))
            , Storage(family.GetStorage())
            , Codec(ExtractDbCodec(family))
            , Cache(ExtractDbCache(family))
            , Room(new TStorageRoom(family.GetRoom()))
            , Name(family.GetName())
        {
        }

        void Update(const NKikimrSchemeOp::TFamilyDescription& family) {
            if (family.HasColumnCodec()) {
                ColumnCodec = family.GetColumnCodec();
                Codec = ToDbCodec(ColumnCodec);
            }
            if (family.HasColumnCache()) {
                ColumnCache = family.GetColumnCache();
                Cache = ToDbCache(ColumnCache);
            }
            if (family.GetStorageConfig().HasDataThreshold()) {
                OuterThreshold = SaveGetThreshold(family.GetStorageConfig().GetDataThreshold());
            }
            if (family.GetStorageConfig().GetExternalThreshold()) {
                ExternalThreshold = SaveGetThreshold(family.GetStorageConfig().GetExternalThreshold());
            }
            if (family.HasStorage()) {
                Storage = family.GetStorage();
            }
            Room.Reset(new TStorageRoom(family.GetRoom()));
        }

        static ui32 SaveGetThreshold(ui32 value) {
            return 0 == value ? Max<ui32>() : value;
        }

        void Update(TStorageRoom::TPtr room) {
            if (room->GetId() != Room->GetId()) {
                return;
            }

            Room = room;
        }

        NKikimrSchemeOp::EColumnCodec ColumnCodec;
        NKikimrSchemeOp::EColumnCache ColumnCache;
        ui32 OuterThreshold;
        ui32 ExternalThreshold;
        NKikimrSchemeOp::EColumnStorage Storage;


        ECodec Codec;
        ECache Cache;
        TStorageRoom::TPtr Room;

        ui32 MainChannel() const {
            if (!*Room) {
                return MainChannelByStorageEnum();
            }

            return Room->GetChannel(NKikimrStorageSettings::TChannelPurpose::Data, 1);
        }

        ui32 MainChannelByStorageEnum() const {
            switch (Storage) {
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext1:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                    return 2;
                default:
                    break;
            }
            return 1;
        }

        ui32 OuterChannel() const {
            if (!*Room) {
                return OuterChannelByStorageEnum();
            }

            return Room->GetChannel(NKikimrStorageSettings::TChannelPurpose::Data, 1);
        }

        ui32 OuterChannelByStorageEnum() const {
            switch (Storage) {
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Med2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                    return 2;
                default:
                    break;
            }
            return MainChannelByStorageEnum();
        }

        ui32 ExternalChannel() const {
            if (!*Room) {
                return ExternalChannelByStorageEnum();
            }

            return Room->GetChannel(NKikimrStorageSettings::TChannelPurpose::External, 1);
        }

        ui32 ExternalChannelByStorageEnum() const {
            switch (Storage) {
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Med2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorageTest_1_2_1k:
                    return 2;
                default:
                    break;
            }
            return 1;
        }

        ui32 GetOuterThreshold() const {
            switch (Storage) {
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Med2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                    return 12*1024;
                case NKikimrSchemeOp::EColumnStorage::ColumnStorageTest_1_2_1k:
                    return 512;
                default:
                    break;
            }
            return OuterThreshold;
        }

        ui32 GetExternalThreshold() const {
            switch (Storage) {
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Ext1:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext1:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Med2Ext2:
                case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                    return 512*1024;
                case NKikimrSchemeOp::EColumnStorage::ColumnStorageTest_1_2_1k:
                    return 1024;
                default:
                    break;
            }
            return ExternalThreshold;
        }

        TString GetName() const {
            if (Name)
                return Name;
            return "default";
        }

        ui32 GetRoomId() const {
            return Room->GetId();
        }

    private:
        TString Name;

        static ECodec ExtractDbCodec(const NKikimrSchemeOp::TFamilyDescription& family) {
            if (family.HasColumnCodec())
                return ToDbCodec(family.GetColumnCodec());
            if (family.GetCodec() == 1) // legacy
                return ECodec::LZ4;
            return ECodec::Plain;
        }

        static ECodec ToDbCodec(NKikimrSchemeOp::EColumnCodec codec) {
            switch (codec) {
                case NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain:
                    return ECodec::Plain;
                case NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4:
                case NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD: // FIXME: not supported
                    return ECodec::LZ4;
                // keep no default
            }
            Y_ABORT("unexpected");
        }

        static ECache ExtractDbCache(const NKikimrSchemeOp::TFamilyDescription& family) {
            if (family.HasInMemory() && family.GetInMemory()) // legacy
                return ECache::Ever;
            return ECache::None;
        }

        static ECache ToDbCache(NKikimrSchemeOp::EColumnCache cache) {
            switch (cache) {
                case NKikimrSchemeOp::EColumnCache::ColumnCacheNone:
                    return ECache::None;
                case NKikimrSchemeOp::EColumnCache::ColumnCacheOnce:
                    return ECache::Once;
                case NKikimrSchemeOp::EColumnCache::ColumnCacheEver:
                    return ECache::Ever;
                // keep no default
            }
            Y_ABORT("unexpected");
        }
    };

    struct TUserColumn {
        NScheme::TTypeInfo Type;
        TString TypeMod;
        TString Name;
        bool IsKey;
        ui32 Family = 0;
        bool NotNull = false;

        TUserColumn(NScheme::TTypeInfo type, TString typeMod, TString name, bool isKey = false)
            : Type(type)
            , TypeMod(typeMod)
            , Name(name)
            , IsKey(isKey)
        {}

        TUserColumn()
            : IsKey(false)
        {}
    };

    struct TTableIndex {
        using EType = NKikimrSchemeOp::EIndexType;
        using EState = NKikimrSchemeOp::EIndexState;

        EType Type;
        EState State;
        TVector<ui32> KeyColumnIds;
        TVector<ui32> DataColumnIds;

        TTableIndex() = default;

        TTableIndex(const NKikimrSchemeOp::TIndexDescription& indexDesc, const TMap<ui32, TUserColumn>& columns)
            : Type(indexDesc.GetType())
            , State(indexDesc.GetState())
        {
            if (Type != EType::EIndexTypeGlobalAsync) {
                return;
            }
            THashMap<TStringBuf, ui32> nameToId;
            for (const auto& [id, column] : columns) {
                Y_DEBUG_ABORT_UNLESS(!nameToId.contains(column.Name));
                nameToId.emplace(column.Name, id);
            }

            auto fillColumnIds = [&nameToId](const auto& columnNames, TVector<ui32>& columnIds) {
                columnIds.reserve(columnNames.size());
                for (const auto& columnName : columnNames) {
                    auto it = nameToId.find(columnName);
                    Y_ABORT_UNLESS(it != nameToId.end());
                    columnIds.push_back(it->second);
                }
            };

            fillColumnIds(indexDesc.GetKeyColumnNames(),  KeyColumnIds);
            fillColumnIds(indexDesc.GetDataColumnNames(), DataColumnIds);
        }

        static void Rename(NKikimrSchemeOp::TIndexDescription& indexDesc, const TString& newName) {
            indexDesc.SetName(newName);
        }
    };

    struct TCdcStream {
        using EMode = NKikimrSchemeOp::ECdcStreamMode;
        using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
        using EState = NKikimrSchemeOp::ECdcStreamState;

        TString Name;
        EMode Mode;
        EFormat Format;
        EState State;
        bool VirtualTimestamps = false;
        TDuration ResolvedTimestampsInterval;
        TMaybe<TString> AwsRegion;

        TCdcStream() = default;

        TCdcStream(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc)
            : Name(streamDesc.GetName())
            , Mode(streamDesc.GetMode())
            , Format(streamDesc.GetFormat())
            , State(streamDesc.GetState())
            , VirtualTimestamps(streamDesc.GetVirtualTimestamps())
            , ResolvedTimestampsInterval(TDuration::MilliSeconds(streamDesc.GetResolvedTimestampsIntervalMs()))
        {
            if (const auto& awsRegion = streamDesc.GetAwsRegion()) {
                AwsRegion = awsRegion;
            }
        }
    };

    struct TReplicationConfig {
        NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode Mode;
        NKikimrSchemeOp::TTableReplicationConfig::EConsistency Consistency;

        TReplicationConfig()
            : Mode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE)
            , Consistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_UNKNOWN)
        {
        }

        TReplicationConfig(const NKikimrSchemeOp::TTableReplicationConfig& config)
            : Mode(config.GetMode())
            , Consistency(config.GetConsistency())
        {
        }

        bool HasWeakConsistency() const {
            return Consistency == NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK;
        }

        bool HasStrongConsistency() const {
            return Consistency == NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_STRONG;
        }

        void Serialize(NKikimrSchemeOp::TTableReplicationConfig& proto) const {
            proto.SetMode(Mode);
            proto.SetConsistency(Consistency);
        }
    };

    struct TStats {
        NTable::TStats DataStats;
        ui64 IndexSize = 0;
        ui64 MemRowCount = 0;
        ui64 MemDataSize = 0;
        TInstant AccessTime;
        TInstant UpdateTime;
        TInstant LastFullCompaction;
        THashSet<ui64> PartOwners;
        ui64 PartCount = 0;
        ui64 SearchHeight = 0;
        TInstant StatsUpdateTime;
        ui64 DataSizeResolution = 0;
        ui64 RowCountResolution = 0;
        ui32 HistogramBucketsCount = 0;
        ui64 BackgroundCompactionRequests = 0;
        ui64 BackgroundCompactionCount = 0;
        ui64 CompactBorrowedCount = 0;
        NTable::TKeyAccessSample AccessStats;

        void Update(NTable::TStats&& dataStats, ui64 indexSize, THashSet<ui64>&& partOwners, ui64 partCount, TInstant statsUpdateTime) {
            DataStats = dataStats;
            IndexSize = indexSize;
            PartOwners = partOwners;
            PartCount = partCount;
            StatsUpdateTime = statsUpdateTime;
        }
    };

    struct TSpecialUpdate {
        bool HasUpdates = false;

        ui32 ColIdTablet = Max<ui32>();
        ui32 ColIdEpoch = Max<ui32>();
        ui32 ColIdUpdateNo = Max<ui32>();

        ui64 Tablet = 0;
        ui64 Epoch = 0;
        ui64 UpdateNo = 0;
    };

    ui32 LocalTid = Max<ui32>();
    ui32 ShadowTid = 0;
    TString Name;
    TString Path;
    TMap<ui32, TStorageRoom::TPtr> Rooms;
    TMap<ui32, TUserFamily> Families;
    TMap<ui32, TUserColumn> Columns;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TVector<ui32> KeyColumnIds;
    TSerializedTableRange Range;
    TReplicationConfig ReplicationConfig;
    bool IsBackup = false;

    TMap<TPathId, TTableIndex> Indexes;

    template <typename TCallback>
    void ForAsyncIndex(const TPathId& pathId, TCallback&& callback) const {
        if (AsyncIndexCount == 0) {
            return;
        }
        auto it = Indexes.find(pathId);
        if (it != Indexes.end() && it->second.Type == TTableIndex::EType::EIndexTypeGlobalAsync) {
            callback(it->second);
        }
    }

    template <typename TCallback>
    void ForEachAsyncIndex(TCallback&& callback) const {
        if (AsyncIndexCount == 0) {
            return;
        }
        for (const auto& [pathId, index] : Indexes) {
            if (index.Type == TTableIndex::EType::EIndexTypeGlobalAsync) {
                callback(pathId, index);
            }
        }
    }

    TMap<TPathId, TCdcStream> CdcStreams;
    ui32 AsyncIndexCount = 0;
    ui32 JsonCdcStreamCount = 0;

    // Tablet thread access only, updated in-place
    mutable TStats Stats;
    mutable bool StatsUpdateInProgress = false;
    mutable bool StatsNeedUpdate = true;
    mutable NTable::TDatabase::TChangeCounter LastTableChange;
    mutable TMonotonic LastTableChangeTimestamp;

    ui32 SpecialColTablet = Max<ui32>();
    ui32 SpecialColEpoch = Max<ui32>();
    ui32 SpecialColUpdateNo = Max<ui32>();

    TUserTable() { }

    TUserTable(ui32 localTid, const NKikimrSchemeOp::TTableDescription& descr, ui32 shadowTid); // for create
    TUserTable(const TUserTable& table, const NKikimrSchemeOp::TTableDescription& descr); // for alter

    void ApplyCreate(NTabletFlatExecutor::TTransactionContext& txc, const TString& tableName,
                     const NKikimrSchemeOp::TPartitionConfig& partConfig) const;
    void ApplyCreateShadow(NTabletFlatExecutor::TTransactionContext& txc, const TString& tableName,
                     const NKikimrSchemeOp::TPartitionConfig& partConfig) const;
    void ApplyAlter(NTabletFlatExecutor::TTransactionContext& txc, const TUserTable& oldTable,
                    const NKikimrSchemeOp::TTableDescription& alter, TString& strError);
    void ApplyDefaults(NTabletFlatExecutor::TTransactionContext& txc) const;

    void Fix_KIKIMR_17222(NTable::TDatabase& db) const;

    TTableRange GetTableRange() const { return Range.ToTableRange(); }
    const TString& GetSchema() const { return Schema; }

    void GetSchema(NKikimrSchemeOp::TTableDescription& description) const {
        bool ok = description.ParseFromArray(Schema.data(), Schema.size());
        Y_ABORT_UNLESS(ok);
    }

    void SetSchema(const NKikimrSchemeOp::TTableDescription& description) {
        Schema.clear();
        Y_PROTOBUF_SUPPRESS_NODISCARD description.SerializeToString(&Schema);
    }

    void SetPath(const TString &path);

    ui64 GetTableSchemaVersion() const { return TableSchemaVersion; }
    void SetTableSchemaVersion(ui64 schemaVersion);
    bool ResetTableSchemaVersion();

    void AddIndex(const NKikimrSchemeOp::TIndexDescription& indexDesc);
    void SwitchIndexState(const TPathId& indexPathId, TTableIndex::EState state);
    void DropIndex(const TPathId& indexPathId);
    bool HasAsyncIndexes() const;

    void AddCdcStream(const NKikimrSchemeOp::TCdcStreamDescription& streamDesc);
    void SwitchCdcStreamState(const TPathId& streamPathId, TCdcStream::EState state);
    void DropCdcStream(const TPathId& streamPathId);
    bool HasCdcStreams() const;
    bool NeedSchemaSnapshots() const;

    bool IsReplicated() const;

private:
    void DoApplyCreate(NTabletFlatExecutor::TTransactionContext& txc, const TString& tableName, bool shadow,
            const NKikimrSchemeOp::TPartitionConfig& partConfig) const;

    void Fix_KIKIMR_17222(NTable::TDatabase& db, ui32 tid) const;

private:
    TString Schema;
    ui64 TableSchemaVersion = 0;

    void CheckSpecialColumns();
    void AlterSchema();
    void ParseProto(const NKikimrSchemeOp::TTableDescription& descr);
};

}}
