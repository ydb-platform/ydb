#pragma once

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

#include <atomic>

namespace NEtcd {

struct TSharedStuff {
    using TPtr = std::shared_ptr<TSharedStuff>;
    static TPtr Get();

    std::unique_ptr<NYdb::NQuery::TQueryClient> Client;
    std::atomic_int64_t Revision = 0LL;
};

}
