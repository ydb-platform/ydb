#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoFqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFqPrivateCreateRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFqPrivateDeleteRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);

} // namespace NGRpcService
} // namespace NKikimr
