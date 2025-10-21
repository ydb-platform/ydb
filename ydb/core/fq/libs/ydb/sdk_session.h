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
        NFq::ISession::TTxControl txControl,
        std::shared_ptr<NYdb::TParamsBuilder> paramsBuilder,
        NYdb::NTable::TExecDataQuerySettings execDataQuerySettings = NYdb::NTable::TExecDataQuerySettings()) override {
        NYdb::NTable::TTxControl tx = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW());
        if (txControl.SnapshotRead_) {
            tx = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SnapshotRO());
        }
        if (txControl.Continue_) {
            tx = NYdb::NTable::TTxControl::Tx(*Transaction);
        }
        if (txControl.Commit_) {
            tx = tx.CommitTx();
        }
        Cerr << "ExecuteDataQuery " << sql << Endl;
        Cerr << "Begin_ " << txControl.Begin_ << Endl;
        Cerr << "Continue_ " << txControl.Continue_ << Endl;
        Cerr << "Commit_ " << txControl.Commit_ << Endl;
    
        if (paramsBuilder) {
            return Session.ExecuteDataQuery(sql, tx, paramsBuilder->Build(), execDataQuerySettings);
        } else {
            return Session.ExecuteDataQuery(sql, tx, execDataQuerySettings);
        }
    }

    NYdb::TAsyncStatus Rollback() override {
        auto future = Transaction->Rollback();
        Transaction = std::nullopt;
        return future;
    }

    NYdb::TAsyncStatus CreateTable(const std::string& /*db*/, const std::string& path, NYdb::NTable::TTableDescription&& tableDesc) override {
        return Session.CreateTable(path, std::move(tableDesc));
    }

    NYdb::TAsyncStatus DropTable( const std::string& path) override {
        return Session.DropTable(path);
    }

    void UpdateTransaction(std::optional<NYdb::NTable::TTransaction> transaction) override {
        Transaction = transaction;
    }

    bool HasActiveTransaction() override {
        return Transaction && Transaction->IsActive();
    }

private: 
    NYdb::NTable::TSession Session;
    std::optional<NYdb::NTable::TTransaction> Transaction;
};

} // namespace NFq
