#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoAlterTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCreateTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDropTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCopyTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCopyTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoRenameTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDeleteSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKeepAliveRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoReadTableRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoReadRowsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoExplainDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoPrepareDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExecuteSchemeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoBeginTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoCommitTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoRollbackTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTableOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoBulkUpsertRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExecuteScanQueryRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

}
}
