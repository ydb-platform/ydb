#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/bridge_storage.pb.h>

namespace NKikimr {

    struct TEvBlobStorage::TEvControllerQueryBridgeStorage: TEventPB<TEvControllerQueryBridgeStorage,
                                                                     NKikimrBridge::TStorageQuery, TEvBlobStorage::EvControllerQueryBridgeStorage> {};

    struct TEvBlobStorage::TEvControllerQueryBridgeStorageResult: TEventPB<TEvControllerQueryBridgeStorageResult,
                                                                           NKikimrBridge::TStorageState, TEvBlobStorage::EvControllerQueryBridgeStorageResult> {};

} // namespace NKikimr
