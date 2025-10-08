#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NFq {

struct ISession : public TThrRefBase{

    using TPtr = TIntrusivePtr<ISession>;

    virtual NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> params,
        NYdb::NTable::TTxControl txControl,
        NYdb::NTable::TExecDataQuerySettings execDataQuerySettings = NYdb::NTable::TExecDataQuerySettings()) = 0;
};

} // namespace NFq
