#pragma once

#include "defs.h"

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NStorage {

    struct TEvNodeConfigPush
        : TEventPB<TEvNodeConfigPush, NKikimrBlobStorage::TEvNodeConfigPush, TEvBlobStorage::EvNodeConfigPush>
    {
        TEvNodeConfigPush() = default;

        bool IsUseful() const {
            return Record.BoundNodesSize() || Record.DeletedBoundNodeIdsSize();
        }
    };

    struct TEvNodeConfigReversePush
        : TEventPB<TEvNodeConfigReversePush, NKikimrBlobStorage::TEvNodeConfigReversePush, TEvBlobStorage::EvNodeConfigReversePush>
    {
        TEvNodeConfigReversePush() = default;

        TEvNodeConfigReversePush(ui32 rootNodeId, const NKikimrBlobStorage::TStorageConfig *committedConfig) {
            Record.SetRootNodeId(rootNodeId);
            if (committedConfig) {
                Record.MutableCommittedStorageConfig()->CopyFrom(*committedConfig);
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

} // NKikimr::NStorage
