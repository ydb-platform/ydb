#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <atomic>

namespace NEtcd {

constexpr bool NotifyWatchtower = true;

constexpr auto Endless = "\0"sv;

constexpr auto DataSizeLimit = 59999999ULL;

struct TSharedStuff {
    using TPtr = std::shared_ptr<TSharedStuff>;
    using TWeakPtr = std::weak_ptr<TSharedStuff>;

    std::unique_ptr<NYdb::NQuery::TQueryClient> Client;
    std::atomic<i64> Revision = 0LL;
    NActors::TActorSystem* ActorSystem = nullptr;
    NActors::TActorId Watchtower, MainGate, HolderHouse;
    std::string TablePrefix;

    void UpdateRevision(i64 revision);
};

std::string IncrementKey(std::string key);
std::string DecrementKey(std::string key);

std::ostream& DumpKeyRange(std::ostream& out, std::string_view key, std::string_view end = {});

}
