#pragma once

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

void DoAlterTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCreateTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDropTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCopyTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCopyTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoRenameTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDeleteSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoKeepAliveRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoReadTableRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoExplainDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoPrepareDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoExecuteSchemeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoBeginTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoCommitTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoRollbackTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDescribeTableOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoBulkUpsertRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoExecuteScanQueryRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);

void DoAddOffsetsToTransaction(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
