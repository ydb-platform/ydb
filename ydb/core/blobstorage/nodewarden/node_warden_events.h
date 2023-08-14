#pragma once

#include "defs.h"

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NStorage {

    struct TEvNodeConfigPush
        : TEventPB<TEvNodeConfigPush, NKikimrBlobStorage::TEvNodeConfigPush, TEvBlobStorage::EvNodeConfigPush>
    {
        TEvNodeConfigPush() = default;

        // ctor for initial push request
        TEvNodeConfigPush(const NKikimrBlobStorage::TStorageConfig *config, const THashMap<ui32, ui32>& boundNodeIds) {
            if (config) {
                Record.MutableStorageConfig()->CopyFrom(*config);
            }
            for (const auto [nodeId, counter] : boundNodeIds) {
                Record.AddNewBoundNodeIds(nodeId);
            }
            Record.SetInitial(true);
        }
    };

    struct TEvNodeConfigReversePush
        : TEventPB<TEvNodeConfigReversePush, NKikimrBlobStorage::TEvNodeConfigReversePush, TEvBlobStorage::EvNodeConfigReversePush>
    {
        TEvNodeConfigReversePush() = default;

        TEvNodeConfigReversePush(const NKikimrBlobStorage::TStorageConfig *config, ui32 rootNodeId) {
            if (config) {
                Record.MutableStorageConfig()->CopyFrom(*config);
            }
            Record.SetRootNodeId(rootNodeId);
        }

        static std::unique_ptr<TEvNodeConfigReversePush> MakeRejected() {
            auto res = std::make_unique<TEvNodeConfigReversePush>();
            res->Record.SetRejected(true);
            return res;
        }
    };

    struct TEvNodeConfigUnbind
        : TEventPB<TEvNodeConfigUnbind, NKikimrBlobStorage::TEvNodeConfigUnbind, TEvBlobStorage::EvNodeConfigUnbind>
    {
    };

} // NKikimr::NStorage
