#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>

#include <atomic>

namespace NEtcd {

constexpr auto Endless = "\0"sv;

constexpr auto DataSizeLimit = 59999999ULL;

struct TSharedStuff {
    using TPtr = std::shared_ptr<TSharedStuff>;
    using TWeakPtr = std::weak_ptr<TSharedStuff>;

    std::unique_ptr<NYdb::NQuery::TQueryClient> Client;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;

    std::atomic<i64> Revision = 0LL;
    NActors::TActorSystem* ActorSystem = nullptr;
    NActors::TActorId MainGate, HolderHouse;
    std::string Folder, TablePrefix;

    void UpdateRevision(i64 revision);
};

std::string IncrementKey(std::string key);
std::string DecrementKey(std::string key);

std::ostream& DumpKeyRange(std::ostream& out, std::string_view key, std::string_view end = {});

}
