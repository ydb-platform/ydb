#pragma once

#include <ydb/library/actors/core/actorsystem.h>

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
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter);

    bool IncRequest();
    void DecRequest();
protected:
    virtual void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) = 0;

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NYdbGrpc::TGlobalLimiter* Limiter_ = nullptr;
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
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override {
        TBase::SetGlobalLimiterHandle(limiter);
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
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override {
        TBase::SetGlobalLimiterHandle(limiter);
    }

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
};

} // namespace NGRpcService
} // namespace NKikimr
