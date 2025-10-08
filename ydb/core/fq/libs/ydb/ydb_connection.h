#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/table_client.h>

namespace NFq {

struct IYdbConnection : public TThrRefBase {

    using TPtr = TIntrusivePtr<IYdbConnection>;

    virtual IYdbTableClient::TPtr GetYdbTableClient() = 0;
    virtual TString GetTablePathPrefix() = 0;
    virtual TString GetDb() = 0;
};

//using IYdbConnectionPtr = IYdbConnection::TPtr;


} // namespace NFq
