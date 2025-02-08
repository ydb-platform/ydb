#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/actors/core/actorid.h>

#include <atomic>

namespace NEtcd {

struct TSharedStuff {
    using TPtr = std::shared_ptr<TSharedStuff>;

    std::unique_ptr<NYdb::NQuery::TQueryClient> Client;
    std::atomic_int64_t Revision = 0LL, Lease = 0LL;
    NActors::TActorId Watchtower;
};

TString DecrementKey(TString key);

constexpr bool NotifyWatchtower = true;

}
