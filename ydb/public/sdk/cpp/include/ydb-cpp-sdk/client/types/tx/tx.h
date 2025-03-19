#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fwd.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::inline Dev {

using TPrecommitTransactionCallback = std::function<TAsyncStatus ()>;

class ITransactionBase {
public:
    virtual const std::string& GetId() const = 0;
    virtual const std::string& GetSessionId() const = 0;

    virtual void AddPrecommitCallback(TPrecommitTransactionCallback cb) = 0;

    virtual ~ITransactionBase() = default;
};

} // namespace NYdb
