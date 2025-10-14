#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/fq/libs/ydb/session.h>
#include <ydb/core/fq/libs/ydb/query_actor.h>

#include <ydb/library/table_creator/table_creator.h>

namespace NFq {


struct TSdkSession : public ISession { 

    TSdkSession(NYdb::NTable::TSession session)
    : Session(session) {
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
        const TString& sql,
        std::shared_ptr<NYdb::TParamsBuilder> paramsBuilder,
        NYdb::NTable::TTxControl txControl,
        NYdb::NTable::TExecDataQuerySettings execDataQuerySettings = NYdb::NTable::TExecDataQuerySettings()) override {
        auto params = paramsBuilder->Build();
        return Session.ExecuteDataQuery(sql, txControl, params, execDataQuerySettings);
    }

    void CommitTransaction() override {
        // TODO
    }

    NYdb::TAsyncStatus RollbackTransaction() override {
        // TODO
        return NThreading::MakeFuture(NYdb::TStatus{NYdb::EStatus::SUCCESS, {}});
    }

    NYdb::TAsyncStatus CreateTable(const std::string& /*db*/, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc) override {
        return Session.CreateTable(path, std::move(tableDesc));
    }

    NYdb::TAsyncStatus DropTable( const std::string& path) override {
        return Session.DropTable(path);
    }

private: 
    NYdb::NTable::TSession Session;
};

} // namespace NFq
