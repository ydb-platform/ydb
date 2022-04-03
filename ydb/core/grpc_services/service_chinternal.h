#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoReadColumnsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoGetShardLocationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoKikhouseDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoKikhouseCreateSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoKikhouseRefreshSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoKikhouseDiscardSnapshotRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
