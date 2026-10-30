#pragma once

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/generic/string.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace NYdb::NTests {

class TMockQueryService : public Ydb::Query::V1::QueryService::Service {
public:
    TDuration AttachDelay = TDuration::Zero();
    std::atomic_uint CreateSessionRequests = 0;
    std::atomic_uint ClosedSessions = 0;
    std::function<void(grpc::ServerWriter<Ydb::Query::SessionState>*)> OnAttach;
    std::string OnAttachSessionId = "fake-query-session-id-1";
    grpc::Status AttachStatus = grpc::Status::OK;
    Ydb::StatusIds::StatusCode TxStatus = Ydb::StatusIds::SUCCESS;
    grpc::StatusCode TxRpcStatus = grpc::StatusCode::OK;

    grpc::Status CreateSession(
        grpc::ServerContext*,
        const Ydb::Query::CreateSessionRequest*,
        Ydb::Query::CreateSessionResponse* response) override
    {
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->set_session_id("fake-query-session-id-" + std::to_string(++CreateSessionRequests));
        response->set_node_id(1);
        return grpc::Status::OK;
    }

    grpc::Status AttachSession(
        grpc::ServerContext* context,
        const Ydb::Query::AttachSessionRequest* request,
        grpc::ServerWriter<Ydb::Query::SessionState>* writer) override
    {
        Y_DEFER { ++ClosedSessions; };
        if (AttachDelay != TDuration::Zero()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(AttachDelay.MilliSeconds()));
        }
        Ydb::Query::SessionState state;
        state.set_status(Ydb::StatusIds::SUCCESS);
        writer->Write(state);
        if (OnAttach && request->session_id() == OnAttachSessionId) {
            OnAttach(writer);
            if (!AttachStatus.ok()) {
                return AttachStatus;
            }
        }
        while (!context->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return grpc::Status::OK;
    }

    grpc::Status BeginTransaction(grpc::ServerContext*, const Ydb::Query::BeginTransactionRequest*,
                                  Ydb::Query::BeginTransactionResponse* response) override {
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->mutable_tx_meta()->set_id("fake-query-transaction-id");
        return grpc::Status::OK;
    }

    grpc::Status CommitTransaction(grpc::ServerContext*, const Ydb::Query::CommitTransactionRequest*,
                                   Ydb::Query::CommitTransactionResponse* response) override {
        response->set_status(TxStatus);
        return grpc::Status(TxRpcStatus, "mock transaction status");
    }

    grpc::Status RollbackTransaction(grpc::ServerContext*, const Ydb::Query::RollbackTransactionRequest*,
                                     Ydb::Query::RollbackTransactionResponse* response) override {
        response->set_status(TxStatus);
        return grpc::Status(TxRpcStatus, "mock transaction status");
    }

};

template <class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    return grpc::ServerBuilder()
        .AddListeningPort(TString{address}, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
}

} // namespace NYdb::NTests
