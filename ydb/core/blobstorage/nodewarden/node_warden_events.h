#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NStorage {

    struct TEvNodeConfigPush
        : TEventPB<TEvNodeConfigPush, NKikimrBlobStorage::TEvNodeConfigPush, TEvBlobStorage::EvNodeConfigPush>
    {
        TEvNodeConfigPush() = default;

        bool IsUseful() const {
            return Record.BoundNodesSize() || Record.DeletedBoundNodeIdsSize() || Record.HasCacheUpdate();
        }
    };

    struct TEvNodeConfigReversePush
        : TEventPB<TEvNodeConfigReversePush, NKikimrBlobStorage::TEvNodeConfigReversePush, TEvBlobStorage::EvNodeConfigReversePush>
    {
        TEvNodeConfigReversePush() = default;

        TEvNodeConfigReversePush(ui32 rootNodeId, const NKikimrBlobStorage::TStorageConfig *committedConfig,
                bool recurseConfigUpdate) {
            Record.SetRootNodeId(rootNodeId);
            if (committedConfig) {
                Record.MutableCommittedStorageConfig()->CopyFrom(*committedConfig);
            }
            if (recurseConfigUpdate) {
                Record.SetRecurseConfigUpdate(recurseConfigUpdate);
            }
        }

        static std::unique_ptr<TEvNodeConfigReversePush> MakeRejected() {
            auto res = std::make_unique<TEvNodeConfigReversePush>();
            res->Record.SetRejected(true);
            return res;
        }
    };

    struct TEvNodeConfigUnbind
        : TEventPB<TEvNodeConfigUnbind, NKikimrBlobStorage::TEvNodeConfigUnbind, TEvBlobStorage::EvNodeConfigUnbind>
    {};

    struct TEvNodeConfigScatter
        : TEventPB<TEvNodeConfigScatter, NKikimrBlobStorage::TEvNodeConfigScatter, TEvBlobStorage::EvNodeConfigScatter>
    {
        TEvNodeConfigScatter() = default;
    };

    struct TEvNodeConfigGather
        : TEventPB<TEvNodeConfigGather, NKikimrBlobStorage::TEvNodeConfigGather, TEvBlobStorage::EvNodeConfigGather>
    {};

    struct TEvNodeConfigInvokeOnRoot
        : TEventPB<TEvNodeConfigInvokeOnRoot, NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot, TEvBlobStorage::EvNodeConfigInvokeOnRoot>
    {};

    struct TEvNodeConfigInvokeOnRootResult
        : TEventPB<TEvNodeConfigInvokeOnRootResult, NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult, TEvBlobStorage::EvNodeConfigInvokeOnRootResult>
    {};

    struct TEvNodeWardenQueryBaseConfig
        : TEventLocal<TEvNodeWardenQueryBaseConfig, TEvBlobStorage::EvNodeWardenQueryBaseConfig>
    {};

    struct TEvNodeWardenNotifyConfigMismatch
        : TEventLocal<TEvNodeWardenNotifyConfigMismatch, TEvBlobStorage::EvNodeWardenNotifyConfigMismatch> {
        ui32 NodeId;
        ui64 ClusterStateGeneration;
        ui64 ClusterStateGuid;

        TEvNodeWardenNotifyConfigMismatch(ui32 nodeId, ui64 clusterStateGeneration, ui64 clusterStateGuid)
            : NodeId(nodeId)
            , ClusterStateGeneration(clusterStateGeneration)
            , ClusterStateGuid(clusterStateGuid)
        {}
    };

    struct TEvNodeWardenBaseConfig
        : TEventLocal<TEvNodeWardenBaseConfig, TEvBlobStorage::EvNodeWardenBaseConfig>
    {
        NKikimrBlobStorage::TBaseConfig BaseConfig;
    };

    struct TEvNodeWardenDynamicConfigPush
        : TEventPB<TEvNodeWardenDynamicConfigPush, NKikimrBlobStorage::TEvNodeWardenDynamicConfigPush, TEvBlobStorage::EvNodeWardenDynamicConfigPush>
    {};

    struct TEvNodeWardenReadMetadata : TEventLocal<TEvNodeWardenReadMetadata, TEvBlobStorage::EvNodeWardenReadMetadata> {
        TString Path;

        TEvNodeWardenReadMetadata(TString path)
            : Path(std::move(path))
        {}
    };

    struct TEvNodeWardenReadMetadataResult : TEventLocal<TEvNodeWardenReadMetadataResult, TEvBlobStorage::EvNodeWardenReadMetadataResult> {
        std::optional<ui64> Guid;
        NPDisk::EPDiskMetadataOutcome Outcome;
        NKikimrBlobStorage::TPDiskMetadataRecord Record;

        TEvNodeWardenReadMetadataResult(std::optional<ui64> guid, NPDisk::EPDiskMetadataOutcome outcome,
                NKikimrBlobStorage::TPDiskMetadataRecord record)
            : Guid(guid)
            , Outcome(outcome)
            , Record(std::move(record))
        {}
    };

    struct TEvNodeWardenWriteMetadata : TEventLocal<TEvNodeWardenWriteMetadata, TEvBlobStorage::EvNodeWardenWriteMetadata> {
        TString Path;
        NKikimrBlobStorage::TPDiskMetadataRecord Record;

        TEvNodeWardenWriteMetadata(TString path, NKikimrBlobStorage::TPDiskMetadataRecord record)
            : Path(std::move(path))
            , Record(std::move(record))
        {}
    };

    struct TEvNodeWardenWriteMetadataResult : TEventLocal<TEvNodeWardenWriteMetadataResult, TEvBlobStorage::EvNodeWardenWriteMetadataResult> {
        std::optional<ui64> Guid;
        NPDisk::EPDiskMetadataOutcome Outcome;

        TEvNodeWardenWriteMetadataResult(std::optional<ui64> guid, NPDisk::EPDiskMetadataOutcome outcome)
            : Guid(guid)
            , Outcome(outcome)
        {}
    };

    struct TEvNodeWardenUpdateCache : TEventLocal<TEvNodeWardenUpdateCache, TEvBlobStorage::EvNodeWardenUpdateCache> {
        NKikimrBlobStorage::TCacheUpdate CacheUpdate;

        TEvNodeWardenUpdateCache(NKikimrBlobStorage::TCacheUpdate&& cacheUpdate)
            : CacheUpdate(std::move(cacheUpdate))
        {}
    };

    struct TEvNodeWardenQueryCache : TEventLocal<TEvNodeWardenQueryCache, TEvBlobStorage::EvNodeWardenQueryCache> {
        TString Key;
        bool Subscribe;

        TEvNodeWardenQueryCache(TString key, bool subscribe)
            : Key(std::move(key))
            , Subscribe(subscribe)
        {}
    };

    struct TEvNodeWardenQueryCacheResult : TEventLocal<TEvNodeWardenQueryCacheResult, TEvBlobStorage::EvNodeWardenQueryCacheResult> {
        TString Key;
        std::optional<std::tuple<ui32, TString>> GenerationValue;

        TEvNodeWardenQueryCacheResult(TString key, std::optional<std::tuple<ui32, TString>> generationValue)
            : Key(std::move(key))
            , GenerationValue(std::move(generationValue))
        {}
    };

    struct TEvNodeWardenUnsubscribeFromCache : TEventLocal<TEvNodeWardenUnsubscribeFromCache, TEvBlobStorage::EvNodeWardenUnsubscribeFromCache> {
        TString Key;

        TEvNodeWardenUnsubscribeFromCache(TString key)
            : Key(std::move(key))
        {}
    };

    struct TEvNodeWardenManageSyncers
        : TEventLocal<TEvNodeWardenManageSyncers, TEvBlobStorage::EvNodeWardenManageSyncers>
    {
        struct TSyncer {
            ui32 NodeId;
            TGroupId GroupId;
            TBridgePileId TargetBridgePileId;
        };
        std::vector<TSyncer> RunSyncers;

        TEvNodeWardenManageSyncers(std::vector<TSyncer>&& runSyncers)
            : RunSyncers(std::move(runSyncers))
        {}
    };

    struct TEvNodeWardenManageSyncersResult
        : TEventLocal<TEvNodeWardenManageSyncersResult, TEvBlobStorage::EvNodeWardenManageSyncersResult>
    {
        struct TSyncer {
            TGroupId GroupId;
            TBridgePileId TargetBridgePileId;
        };
        std::vector<TSyncer> WorkingSyncers;

        TEvNodeWardenManageSyncersResult(std::vector<TSyncer>&& workingSyncers)
            : WorkingSyncers(std::move(workingSyncers))
        {}
    };

} // NKikimr::NStorage
