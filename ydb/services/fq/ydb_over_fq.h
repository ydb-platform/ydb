#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/public/api/grpc/draft/fq_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbOverFqService
{
public:
    TGRpcYdbOverFqService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger);
protected:
    virtual void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) = 0;

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
};

class TGrpcTableOverFqService
    : public TGRpcYdbOverFqService
    , public NYdbGrpc::TGrpcServiceBase<Ydb::Table::V1::TableService> {
    using TBase = TGRpcYdbOverFqService;
    using TBase::TBase;

public:
    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override {
        TBase::InitService(cq, std::move(logger));
    }

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
};

class TGrpcSchemeOverFqService
    : public TGRpcYdbOverFqService
    , public NYdbGrpc::TGrpcServiceBase<Ydb::Scheme::V1::SchemeService> {
    using TBase = TGRpcYdbOverFqService;
    using TBase::TBase;

public:
    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override {
        TBase::InitService(cq, std::move(logger));
    }

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
};

} // namespace NGRpcService
} // namespace NKikimr
