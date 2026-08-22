#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/session.h>

namespace NFq {

using TOperationFunc = std::function<NYdb::TAsyncStatus(ISession::TPtr)>;

struct IYdbTableClient : public TThrRefBase {

    using TPtr = TIntrusivePtr<IYdbTableClient>;

    virtual NYdb::TAsyncStatus RetryOperation(
        TOperationFunc&& operation,
        const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) = 0;
};

} // namespace NFq
