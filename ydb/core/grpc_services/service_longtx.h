#pragma once
#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoLongTxBeginRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoLongTxCommitRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoLongTxRollbackRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoLongTxWriteRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoLongTxReadRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
