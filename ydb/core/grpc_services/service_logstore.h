#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDropLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAlterLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

void DoCreateLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDropLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoAlterLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
