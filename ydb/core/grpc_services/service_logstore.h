#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoCreateLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAlterLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

void DoCreateLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoAlterLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
