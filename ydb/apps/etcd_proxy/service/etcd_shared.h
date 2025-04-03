#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/actors/core/actorid.h>

#include <atomic>

namespace NEtcd {

constexpr bool NotifyWatchtower = true;

constexpr auto Endless = "\0"sv;

struct TSharedStuff {
    using TPtr = std::shared_ptr<TSharedStuff>;

    std::unique_ptr<NYdb::NQuery::TQueryClient> Client;
    std::atomic<i64> Revision = 0LL, Lease = 0LL;
    NActors::TActorId Watchtower;
};

std::string IncrementKey(std::string key);
std::string DecrementKey(std::string key);

std::ostream& DumpKeyRange(std::ostream& out, std::string_view key, std::string_view end = {});

}
