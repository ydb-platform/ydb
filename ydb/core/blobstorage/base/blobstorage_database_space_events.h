#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    // Space state of databases: BS_CONTROLLER decides whether database's storage is exhausted, local actors subscribe
    // to it through their node's NodeWarden. A database is identified by its domain key, the same as ScopeId of its
    // storage pools.

    struct TEvBlobStorage::TEvControllerSubscribeDatabaseSpace : TEventPB<TEvControllerSubscribeDatabaseSpace,
            NKikimrBlobStorage::TEvControllerSubscribeDatabaseSpace, TEvBlobStorage::EvControllerSubscribeDatabaseSpace> {
        TEvControllerSubscribeDatabaseSpace() = default;
    };

    struct TEvBlobStorage::TEvControllerDatabaseSpaceState : TEventPB<TEvControllerDatabaseSpaceState,
            NKikimrBlobStorage::TEvControllerDatabaseSpaceState, TEvBlobStorage::EvControllerDatabaseSpaceState> {
        TEvControllerDatabaseSpaceState() = default;

        TEvControllerDatabaseSpaceState(TPathId scope, bool exhausted) {
            scope.ToProto(Record.MutableScope());
            Record.SetExhausted(exhausted);
        }
    };

}
