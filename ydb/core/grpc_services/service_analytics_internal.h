#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoYqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility); 
void DoYqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility); 
void DoYqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility); 
void DoYqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility); 

} // namespace NGRpcService
} // namespace NKikimr
