#pragma once

#include "schemeshard_info_types_base.h"
#include "schemeshard_info_types_topic.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>

#include <ydb/public/api/protos/ydb_coordination.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TBlockStorePartitionInfo : public TSimpleRefCount<TBlockStorePartitionInfo> {
    using TPtr = TIntrusivePtr<TBlockStorePartitionInfo>;

    ui32 PartitionId = 0;
    ui64 AlterVersion = 0;
};

struct TBlockStoreVolumeInfo : public TSimpleRefCount<TBlockStoreVolumeInfo> {
    using TPtr = TIntrusivePtr<TBlockStoreVolumeInfo>;

    struct TTabletCache {
        ui64 AlterVersion = 0;
        TVector<TTabletId> Tablets;
    };

    static constexpr size_t NumVolumeTabletChannels = 3;

    ui32 DefaultPartitionCount = 0;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    ui64 AlterVersion = 0;
    ui64 TokenVersion = 0;
    THashMap<TShardIdx, TBlockStorePartitionInfo::TPtr> Shards; // key ShardIdx
    TIntrusivePtr<TBlockStoreVolumeInfo> AlterData;
    TTabletId VolumeTabletId = InvalidTabletId;
    TShardIdx VolumeShardIdx = InvalidShardIdx;
    TString MountToken;
    TTabletCache TabletCache;
    ui32 ExplicitChannelProfileCount = 0;

    static ui32 CalculateDefaultPartitionCount(
        const NKikimrBlockStore::TVolumeConfig& config)
    {
        ui32 c = 0;
        for (const auto& partition: config.GetPartitions()) {
            if (partition.GetType() == NKikimrBlockStore::EPartitionType::Default) {
                ++c;
            }
        }

        return c;
    }

    bool HasVolumeTablet() const { return VolumeTabletId != InvalidTabletId; }

    void PrepareAlter(TIntrusivePtr<TBlockStoreVolumeInfo> alterData) {
        Y_ENSURE(alterData, "No alter data at Alter preparation");
        if (!alterData->DefaultPartitionCount) {
            alterData->DefaultPartitionCount =
                CalculateDefaultPartitionCount(alterData->VolumeConfig);
        }
        alterData->VolumeTabletId = VolumeTabletId;
        alterData->VolumeShardIdx = VolumeShardIdx;
        alterData->AlterVersion = AlterVersion + 1;
        AlterData = alterData;
    }

    void ForgetAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter rollback");
        AlterData.Reset();
    }

    void FinishAlter() {
        Y_ENSURE(AlterData, "No alter data at Alter completion");
        DefaultPartitionCount = AlterData->DefaultPartitionCount;
        ExplicitChannelProfileCount = AlterData->ExplicitChannelProfileCount;
        VolumeConfig.CopyFrom(AlterData->VolumeConfig);
        ++AlterVersion;
        Y_ENSURE(AlterVersion == AlterData->AlterVersion);
        Y_ENSURE(VolumeTabletId == AlterData->VolumeTabletId || !HasVolumeTablet());
        Y_ENSURE(AlterData->HasVolumeTablet());
        Y_ENSURE(AlterData->VolumeShardIdx);
        VolumeTabletId = AlterData->VolumeTabletId;
        VolumeShardIdx = AlterData->VolumeShardIdx;
        AlterData.Reset();
    }

    const TVector<TTabletId>& GetTablets(const THashMap<TShardIdx, TShardInfo>& allShards) {
        if (TabletCache.AlterVersion == AlterVersion) {
            return TabletCache.Tablets;
        }

        TabletCache.Tablets.clear();
        TabletCache.Tablets.resize(DefaultPartitionCount);

        for (const auto& kv : Shards) {
            TShardIdx shardIdx = kv.first;
            const auto& partInfo = *kv.second;

            auto itShard = allShards.find(shardIdx);
            Y_ENSURE(itShard != allShards.end(), "No shard with shardIdx " << shardIdx);
            TTabletId tabletId = itShard->second.TabletID;

            if (partInfo.AlterVersion <= AlterVersion) {
                Y_ENSURE(partInfo.PartitionId < DefaultPartitionCount,
                    "Wrong PartitionId " << partInfo.PartitionId);
                TabletCache.Tablets[partInfo.PartitionId] = tabletId;
            }
        }

        // Verify there are no missing tabletIds
        for (ui32 idx = 0; idx < TabletCache.Tablets.size(); ++idx) {
            TTabletId tabletId = TabletCache.Tablets[idx];
            Y_ENSURE(tabletId, "Unassigned tabletId"
                           << " for partition " << idx
                           << " out of " << TabletCache.Tablets.size()
                           << " TabletCache.AlterVersion" << TabletCache.AlterVersion
                           << " AlterVersion " << AlterVersion);
        }

        TabletCache.AlterVersion = AlterVersion;
        return TabletCache.Tablets;
    }

    TVolumeSpace GetVolumeSpace() const {
        ui64 blockSize = VolumeConfig.GetBlockSize();
        ui64 blockCount = 0;
        for (const auto& partition: VolumeConfig.GetPartitions()) {
            blockCount += partition.GetBlockCount();
        }

        TVolumeSpace space;
        space.Raw += blockCount * blockSize;
        switch (VolumeConfig.GetStorageMediaKind()) {
            case 1: // STORAGE_MEDIA_SSD
                if (VolumeConfig.GetIsSystem()) {
                    space.SSDSystem += blockCount * blockSize; // merged blobs
                } else {
                    space.SSD += blockCount * blockSize; // merged blobs
                    space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                }
                break;
            case 2: // STORAGE_MEDIA_HYBRID
                space.HDD += blockCount * blockSize; // merged blobs
                space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                break;
            case 3: // STORAGE_MEDIA_HDD
                space.HDD += blockCount * blockSize; // merged blobs
                space.SSD += (blockCount / 8) * blockSize; // mixed blobs
                break;
            case 4: // STORAGE_MEDIA_SSD_NONREPLICATED
                space.SSDNonrepl += blockCount * blockSize; // blocks are stored directly
                break;
        }

        if (AlterData) {
            auto altSpace = AlterData->GetVolumeSpace();
            space.Raw = Max(space.Raw, altSpace.Raw);
            space.SSD = Max(space.SSD, altSpace.SSD);
            space.HDD = Max(space.HDD, altSpace.HDD);
            space.SSDNonrepl = Max(space.SSDNonrepl, altSpace.SSDNonrepl);
            space.SSDSystem = Max(space.SSDSystem, altSpace.SSDSystem);
        }

        return space;
    }
};

struct TFileStoreInfo : public TSimpleRefCount<TFileStoreInfo> {
    using TPtr = TIntrusivePtr<TFileStoreInfo>;

    TShardIdx IndexShardIdx = InvalidShardIdx;
    TTabletId IndexTabletId = InvalidTabletId;

    NKikimrFileStore::TConfig Config;
    ui64 Version = 0;

    THolder<NKikimrFileStore::TConfig> AlterConfig;
    ui64 AlterVersion = 0;

    void PrepareAlter(const NKikimrFileStore::TConfig& alterConfig) {
        Y_ENSURE(!AlterConfig);
        Y_ENSURE(!AlterVersion);

        AlterConfig = MakeHolder<NKikimrFileStore::TConfig>();
        AlterConfig->CopyFrom(alterConfig);

        Y_ENSURE(!AlterConfig->GetBlockSize());
        AlterConfig->SetBlockSize(Config.GetBlockSize());

        AlterVersion = Version + 1;
    }

    void ForgetAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        AlterConfig.Reset();
        AlterVersion = 0;
    }

    void FinishAlter() {
        Y_ENSURE(AlterConfig);
        Y_ENSURE(AlterVersion);

        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);

        ForgetAlter();
    }

    TFileStoreSpace GetFileStoreSpace() const {
        auto space = GetFileStoreSpace(Config);

        if (AlterConfig) {
            const auto alterSpace = GetFileStoreSpace(*AlterConfig);
            space.SSD = Max(space.SSD, alterSpace.SSD);
            space.HDD = Max(space.HDD, alterSpace.HDD);
            space.SSDSystem = Max(space.SSDSystem, alterSpace.SSDSystem);
        }

        return space;
    }

    static bool ValidateFileStoreConfigSpaceOverflow(ui64 blockSize, ui64 blockCount, TString& errStr) {
        if (blockSize && blockCount > Max<ui64>() / blockSize) {
            errStr = TStringBuilder()
                << "FileStore size overflows ui64: blocks count " << blockCount
                << " * block size " << blockSize
                << " > " << Max<ui64>();
            return false;
        }

        return true;
    }

private:
    TFileStoreSpace GetFileStoreSpace(const NKikimrFileStore::TConfig& config) const {
        const ui64 blockSize = config.GetBlockSize();
        const ui64 blockCount = config.GetBlocksCount();

        TFileStoreSpace space;
        switch (config.GetStorageMediaKind()) {
            case 1: // STORAGE_MEDIA_SSD
                if (config.GetIsSystem()) {
                    space.SSDSystem += blockCount * blockSize;
                } else {
                    space.SSD += blockCount * blockSize;
                }
                break;
            case 2: // STORAGE_MEDIA_HYBRID
            case 3: // STORAGE_MEDIA_HDD
                space.HDD += blockCount * blockSize;
                break;
        }

        return space;
    }
};

struct TKesusInfo : public TSimpleRefCount<TKesusInfo> {
    using TPtr = TIntrusivePtr<TKesusInfo>;

    TShardIdx KesusShardIdx = InvalidShardIdx;
    TTabletId KesusTabletId = InvalidTabletId;
    Ydb::Coordination::Config Config;
    ui64 Version = 0;
    THolder<Ydb::Coordination::Config> AlterConfig;
    ui64 AlterVersion = 0;

    void FinishAlter() {
        Y_ENSURE(AlterConfig, "No alter config at Alter completion");
        Y_ENSURE(AlterVersion, "No alter version at Alter completion");
        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);
        AlterConfig.Reset();
        AlterVersion = 0;
    }
};

struct TTableIndexInfo : public TSimpleRefCount<TTableIndexInfo> {
    using TPtr = TIntrusivePtr<TTableIndexInfo>;
    using EType = NKikimrSchemeOp::EIndexType;
    using EState = NKikimrSchemeOp::EIndexState;

    TTableIndexInfo(ui64 version, EType type, EState state, std::string_view description)
        : AlterVersion(version)
        , Type(type)
        , State(state)
    {
        switch (type) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                // no specialized index description
                Y_ASSERT(description.empty());
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
                // JSON indexes carry a fulltext description only when rowid mode (__ydb_row_id as doc_id)
                // is enabled (the serialized description is then non-empty); legacy JSON indexes have none.
                if (!description.empty()) {
                    auto success = SpecializedIndexDescription
                        .emplace<NKikimrSchemeOp::TFulltextIndexDescription>()
                        .ParseFromString(description);
                    Y_ENSURE(success, description);
                }
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TFulltextIndexDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TBloomFilter>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TBloomNGrammFilter>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeInvalid:
                break;
        }
    }

    TTableIndexInfo(const TTableIndexInfo&) = default;

    TPtr CreateNextVersion() {
        this->AlterData = this->GetNextVersion();
        return this->AlterData;
    }

    TPtr GetNextVersion() const {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TTableIndexInfo(*this);
        ++result->AlterVersion;
        return result;
    }

    TString SerializeDescription() const {
        return std::visit([]<typename T>(const T& v) {
            if constexpr (std::is_same_v<std::monostate, T>) {
                return TString{};
            } else if constexpr (std::is_same_v<NKikimrSchemeOp::TBloomNGrammFilter, T>) {
                TString str;
                Y_ENSURE(v.SerializeToString(&str));
                return str;
            } else {
                TString str{v.SerializeAsString()};
                Y_ENSURE(!str.empty());
                return str;
            }
        }, SpecializedIndexDescription);
    }

    static TPtr NotExistedYet(EType type) {
        return new TTableIndexInfo(0, type, EState::EIndexStateInvalid, {});
    }

    static bool IsLocalIndex(EType type) {
        return type == NKikimrSchemeOp::EIndexTypeLocalBloomFilter
            || type == NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter
            || type == NKikimrSchemeOp::EIndexTypeLocalMinMax
            || type == NKikimrSchemeOp::EIndexTypeLocalCountMinSketch;
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexCreationConfig& config, TString& errMsg) {
        if (!config.KeyColumnNamesSize() && !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "no key columns in index creation config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        if (!IsLocalIndex(config.GetType())) {
            Y_ENSURE(!alterData->IndexKeys.empty());
        }
        alterData->IndexDataColumns.assign(config.GetDataColumnNames().begin(), config.GetDataColumnNames().end());

        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        switch (NTableIndex::GetIndexType(config)) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                // no specialized index description
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
                // JSON indexes carry a fulltext description only when rowid mode (__ydb_row_id as doc_id)
                // is enabled; otherwise there is no specialized index description.
                if (config.HasFulltextIndexDescription()) {
                    alterData->SpecializedIndexDescription = config.GetFulltextIndexDescription();
                }
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
                alterData->SpecializedIndexDescription = config.GetVectorIndexKmeansTreeDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
                alterData->SpecializedIndexDescription = config.GetFulltextIndexDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                alterData->SpecializedIndexDescription = config.GetBloomFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                alterData->SpecializedIndexDescription = config.GetBloomNGrammFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeInvalid:
                errMsg += NTableIndex::InvalidIndexType(config.GetType());
                return nullptr;
        }

        return result;
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexAlteringConfig& config, TString& errMsg) {
        if (!config.HasType() || !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "TIndexAlteringConfig requires a valid local index type to be set";
            return nullptr;
        }

        if (!config.KeyColumnNamesSize() && !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "no key columns in index altering config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        if (!IsLocalIndex(config.GetType())) {
            Y_ENSURE(!alterData->IndexKeys.empty());
        }

        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        switch (NTableIndex::GetIndexType(config)) {
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                alterData->SpecializedIndexDescription = config.GetBloomFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                alterData->SpecializedIndexDescription = config.GetBloomNGrammFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                alterData->SpecializedIndexDescription = std::monostate{};
                break;
            default:
                errMsg += TStringBuilder() << "TIndexAlteringConfig only supports local index types, got: " << NKikimrSchemeOp::EIndexType_Name(config.GetType());
                return nullptr;
        }

        return result;
    }

    ui64 AlterVersion = 1;
    EType Type;
    EState State;

    TVector<TString> IndexKeys;
    TVector<TString> IndexDataColumns;

    TIntrusivePtr<TTableIndexInfo> AlterData = nullptr;

    std::variant<std::monostate,
        NKikimrSchemeOp::TVectorIndexKmeansTreeDescription,
        NKikimrSchemeOp::TFulltextIndexDescription,
        NKikimrSchemeOp::TBloomFilter,
        NKikimrSchemeOp::TBloomNGrammFilter> SpecializedIndexDescription;
};

struct TCdcStreamSettings {
    using TSelf = TCdcStreamSettings;
    using EMode = NKikimrSchemeOp::ECdcStreamMode;
    using EFormat = NKikimrSchemeOp::ECdcStreamFormat;
    using EState = NKikimrSchemeOp::ECdcStreamState;

    #define OPTION(type, name) \
        TSelf&& With##name(type value) && { \
            name = std::move(value); \
            return std::move(*this); \
        } \
        type name;

    OPTION(EMode, Mode);
    OPTION(EFormat, Format);
    OPTION(bool, VirtualTimestamps);
    OPTION(TDuration, ResolvedTimestamps);
    OPTION(bool, SchemaChanges);
    OPTION(TString, AwsRegion);
    OPTION(EState, State);
    OPTION(bool, UserSIDs);
    OPTION(bool, TraceIds);

    #undef OPTION
};

struct TCdcStreamShardStatus {
    NKikimrTxDataShard::TEvCdcStreamScanResponse_EStatus Status;

    explicit TCdcStreamShardStatus(NKikimrTxDataShard::TEvCdcStreamScanResponse_EStatus status)
        : Status(status)
    {}
};

struct TCdcStreamInfo
    : public TCdcStreamSettings
    , public TSimpleRefCount<TCdcStreamInfo>
{
    using TPtr = TIntrusivePtr<TCdcStreamInfo>;

    using TShardStatus = TCdcStreamShardStatus;

    TCdcStreamInfo(ui64 version, TCdcStreamSettings&& settings)
        : TCdcStreamSettings(std::move(settings))
        , AlterVersion(version)
    {}

    TCdcStreamInfo(const TCdcStreamInfo&) = default;

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TCdcStreamInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static TPtr New(TCdcStreamSettings settings) {
        settings.State = EState::ECdcStreamStateInvalid;
        return new TCdcStreamInfo(0, std::move(settings));
    }

    static TPtr Create(const NKikimrSchemeOp::TCdcStreamDescription& desc) {
        TPtr result = New(TCdcStreamSettings()
            .WithMode(desc.GetMode())
            .WithFormat(desc.GetFormat())
            .WithVirtualTimestamps(desc.GetVirtualTimestamps())
            .WithResolvedTimestamps(TDuration::MilliSeconds(desc.GetResolvedTimestampsIntervalMs()))
            .WithSchemaChanges(desc.GetSchemaChanges())
            .WithAwsRegion(desc.GetAwsRegion())
            .WithUserSIDs(desc.GetUserSIDs())
            .WithTraceIds(desc.GetTraceIds())
        );
        TPtr alterData = result->CreateNextVersion();
        alterData->State = EState::ECdcStreamStateReady;
        if (desc.HasState()) {
            alterData->State = desc.GetState();
        }

        return result;
    }

    void Serialize(NKikimrSchemeOp::TCdcStreamDescription& desc) const {
        desc.SetSchemaVersion(AlterVersion);
        desc.SetMode(Mode);
        desc.SetFormat(Format);
        desc.SetVirtualTimestamps(VirtualTimestamps);
        desc.SetResolvedTimestampsIntervalMs(ResolvedTimestamps.MilliSeconds());
        desc.SetSchemaChanges(SchemaChanges);
        desc.SetAwsRegion(AwsRegion);
        desc.SetState(State);
        if (ScanShards) {
            auto& scanProgress = *desc.MutableScanProgress();
            scanProgress.SetShardsTotal(ScanShards.size());
            scanProgress.SetShardsCompleted(DoneShards.size());
        }
        desc.SetUserSIDs(UserSIDs);
        desc.SetTraceIds(TraceIds);
    }

    void FinishAlter() {
        Y_ENSURE(AlterData);

        AlterVersion = AlterData->AlterVersion;
        static_cast<TCdcStreamSettings&>(*this) = static_cast<TCdcStreamSettings&>(*AlterData);

        AlterData.Reset();
    }

    ui64 AlterVersion = 1;
    TIntrusivePtr<TCdcStreamInfo> AlterData = nullptr;

    TMap<TShardIdx, TShardStatus> ScanShards;
    THashSet<TShardIdx> PendingShards;
    THashSet<TShardIdx> InProgressShards;
    THashSet<TShardIdx> DoneShards;
};

struct TSequenceInfo : public TSimpleRefCount<TSequenceInfo> {
    using TPtr = TIntrusivePtr<TSequenceInfo>;

    explicit TSequenceInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    { }

    TSequenceInfo(
        ui64 alterVersion,
        NKikimrSchemeOp::TSequenceDescription&& description,
        NKikimrSchemeOp::TSequenceSharding&& sharding);

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TSequenceInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;
        return result;
    }

    static bool ValidateCreate(const NKikimrSchemeOp::TSequenceDescription& p, TString& err);

    ui64 AlterVersion = 0;
    TIntrusivePtr<TSequenceInfo> AlterData = nullptr;
    NKikimrSchemeOp::TSequenceDescription Description;
    NKikimrSchemeOp::TSequenceSharding Sharding;

    ui64 SequenceShard = 0;
};

struct TReplicationInfo : public TSimpleRefCount<TReplicationInfo> {
    using TPtr = TIntrusivePtr<TReplicationInfo>;

    TReplicationInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {
    }

    TReplicationInfo(ui64 alterVersion, NKikimrSchemeOp::TReplicationDescription&& desc)
        : AlterVersion(alterVersion)
        , Description(std::move(desc))
    {
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(AlterData == nullptr);

        TPtr result = new TReplicationInfo(*this);
        ++result->AlterVersion;
        this->AlterData = result;

        return result;
    }

    static TPtr New() {
        return new TReplicationInfo(0);
    }

    static TPtr Create(NKikimrSchemeOp::TReplicationDescription&& desc) {
        TPtr result = New();
        TPtr alterData = result->CreateNextVersion();
        alterData->Description = std::move(desc);

        return result;
    }

    ui64 AlterVersion = 0;
    TIntrusivePtr<TReplicationInfo> AlterData = nullptr;
    NKikimrSchemeOp::TReplicationDescription Description;
    TShardIdx ControllerShardIdx = InvalidShardIdx;
};

struct TBlobDepotInfo : TSimpleRefCount<TBlobDepotInfo> {
    using TPtr = TIntrusivePtr<TBlobDepotInfo>;

    TBlobDepotInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {}

    TBlobDepotInfo(ui64 alterVersion, const NKikimrSchemeOp::TBlobDepotDescription& desc)
        : AlterVersion(alterVersion)
    {
        Description.CopyFrom(desc);
    }

    TPtr CreateNextVersion() {
        Y_ENSURE(!AlterData);
        AlterData = MakeIntrusive<TBlobDepotInfo>(*this);
        ++AlterData->AlterVersion;
        return AlterData;
    }

    ui64 AlterVersion = 0;
    TPtr AlterData = nullptr;
    TShardIdx BlobDepotShardIdx = InvalidShardIdx;
    TTabletId BlobDepotTabletId = InvalidTabletId;
    NKikimrSchemeOp::TBlobDepotDescription Description;
};

} // namespace NSchemeShard
} // namespace NKikimr
