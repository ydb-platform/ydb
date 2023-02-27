#pragma once
#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoLongTxBeginRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoLongTxCommitRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoLongTxRollbackRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoLongTxWriteRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoLongTxReadRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
