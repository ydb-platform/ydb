#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoDescribeReplication(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTransfer(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoStreamChecksumTablesInternal(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

}
