#pragma once

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>

namespace NYq {

struct TEvControlPlaneStorageInternal {
    // Private
    struct TEvDbRequestResult : public NActors::TEventLocal<TEvDbRequestResult, TEvControlPlaneStorage::EvDbRequestResult> {
        explicit TEvDbRequestResult(const NYdb::TAsyncStatus& status, std::shared_ptr<TVector<NYdb::TResultSet>> resultSets = nullptr)
            : Status(status)
            , ResultSets(std::move(resultSets))
        {
        }

        NYdb::TAsyncStatus Status;
        std::shared_ptr<TVector<NYdb::TResultSet>> ResultSets;
    };
};

}
