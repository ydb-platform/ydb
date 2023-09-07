#pragma once

#include "defs.h"

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NStorage {

    struct TEvNodeConfigPush
        : TEventPB<TEvNodeConfigPush, NKikimrBlobStorage::TEvNodeConfigPush, TEvBlobStorage::EvNodeConfigPush>
    {
        TEvNodeConfigPush() = default;

        // ctor for initial push request
        TEvNodeConfigPush(const THashMap<ui32, ui32>& boundNodeIds, const NKikimrBlobStorage::TStorageConfig& config) {
            for (const auto [nodeId, counter] : boundNodeIds) {
                Record.AddNewBoundNodeIds(nodeId);
            }
            Record.SetInitial(true);
            if (config.GetGeneration()) {
                Record.MutableStorageConfig()->CopyFrom(config);
            }
        }

        bool IsUseful() const {
            return Record.NewBoundNodeIdsSize() || Record.DeletedBoundNodeIdsSize() || Record.HasStorageConfig();
        }
    };

    struct TEvNodeConfigReversePush
        : TEventPB<TEvNodeConfigReversePush, NKikimrBlobStorage::TEvNodeConfigReversePush, TEvBlobStorage::EvNodeConfigReversePush>
    {
        TEvNodeConfigReversePush() = default;

        TEvNodeConfigReversePush(ui32 rootNodeId, const NKikimrBlobStorage::TStorageConfig *config) {
            Record.SetRootNodeId(rootNodeId);
            if (config) {
                Record.MutableStorageConfig()->CopyFrom(*config);
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

    struct TEvUpdateServiceSet : TEventLocal<TEvUpdateServiceSet, TEvBlobStorage::EvUpdateServiceSet> {
        NKikimrBlobStorage::TNodeWardenServiceSet ServiceSet;

        TEvUpdateServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet)
            : ServiceSet(serviceSet)
        {}
    };

} // NKikimr::NStorage
