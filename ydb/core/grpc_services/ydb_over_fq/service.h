#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NYdbOverFq {

// table
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetCreateSessionExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetDeleteSessionExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetKeepAliveExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetDescribeTableExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExplainDataQueryExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId);
std::function<void(std::unique_ptr<IRequestNoOpCtx>, const IFacilityProvider&)> GetStreamExecuteScanQueryExecutor(NActors::TActorId grpcProxyId);

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetListDirectoryExecutor(NActors::TActorId grpcProxyId);

} // namespace NYdbOverFq

} // namespace NKikimr::NGRpcService
