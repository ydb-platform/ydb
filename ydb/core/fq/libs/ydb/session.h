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
    
    virtual void CommitTransaction() = 0;
    virtual NYdb::TAsyncStatus RollbackTransaction() = 0;

    virtual NYdb::TAsyncStatus CreateTable(const std::string& db, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc
        /*const TCreateTableSettings& settings = TCreateTableSettings()*/) = 0;
    
    virtual NYdb::TAsyncStatus DropTable( const std::string& path) = 0;
        
    private: 
    [[maybe_unused]]int Test = 456;
};

} // namespace NFq
