#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoFetchBackupCollectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListBackupCollectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCreateBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoReadBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoUpdateBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDeleteBackupCollectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
