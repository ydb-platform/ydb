#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/bridge_storage.pb.h>

namespace NKikimr {

    inline NKikimrBridge::ESynchronizationStatus GetBridgeSynchronizationStatus(ui64 required, ui64 synced,
            ui64 syncing, ui64 unknown) {
        if (!required) {
            return NKikimrBridge::NOT_REQUIRED;
        }
        if (synced == required) {
            return NKikimrBridge::SYNCED;
        }
        if (syncing) {
            return NKikimrBridge::SYNCING;
        }
        return unknown ? NKikimrBridge::UNKNOWN : NKikimrBridge::PENDING;
    }

    struct TEvBlobStorage::TEvControllerQueryBridgeStorage: TEventPB<TEvControllerQueryBridgeStorage,
                                                                     NKikimrBridge::TStorageQuery, TEvBlobStorage::EvControllerQueryBridgeStorage> {
        static constexpr ui64 MaxResultBytes = 32'000'000;
        static constexpr ui64 MaxVisitedCandidates = 1'000'000;
    };

    struct TEvBlobStorage::TEvControllerQueryBridgeStorageResult: TEventPB<TEvControllerQueryBridgeStorageResult,
                                                                           NKikimrBridge::TStorageState, TEvBlobStorage::EvControllerQueryBridgeStorageResult> {};

} // namespace NKikimr
