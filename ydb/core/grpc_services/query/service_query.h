#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NQuery {

void DoExecuteQuery(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoExecuteScript(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoFetchScriptResults(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoCreateSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoDeleteSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoAttachSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoBeginTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoCommitTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoRollbackTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NQuery

} // namespace NKikimr::NGRpcService
