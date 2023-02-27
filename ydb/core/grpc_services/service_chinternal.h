#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoReadColumnsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoGetShardLocationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKikhouseDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKikhouseCreateSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKikhouseRefreshSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKikhouseDiscardSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
