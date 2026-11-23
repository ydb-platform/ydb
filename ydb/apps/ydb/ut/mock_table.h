#pragma once

#include "mock_env.h"

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

class TMockTableService : public TMockGrpcServiceBase<Ydb::Table::V1::TableService::Service> {
public:
    grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Table::CreateSessionRequest*, Ydb::Table::CreateSessionResponse* response) override {
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("test-session-id");
        FillOperation(response, result);
        return grpc::Status::OK;
    }

    grpc::Status DeleteSession(grpc::ServerContext*, const Ydb::Table::DeleteSessionRequest*, Ydb::Table::DeleteSessionResponse* response) override {
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }

    grpc::Status KeepAlive(grpc::ServerContext*, const Ydb::Table::KeepAliveRequest*, Ydb::Table::KeepAliveResponse* response) override {
        Ydb::Table::KeepAliveResult result;
        result.set_session_status(Ydb::Table::KeepAliveResult::SESSION_STATUS_READY);
        FillOperation(response, result);
        return grpc::Status::OK;
    }

protected:
    template <typename TResponse, typename TResult>
    void FillOperation(TResponse* response, const TResult& result) {
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
    }
};
