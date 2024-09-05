#pragma once
#include "defs.h"

namespace Ydb::Table {

class ExecuteDataQueryRequest;
class ExecuteQueryResult;
class PrepareDataQueryRequest;
class PrepareQueryResult;
class BeginTransactionRequest;
class BeginTransactionResult;
class CommitTransactionRequest;
class RollbackTransactionRequest;
class BulkUpsertRequest;

}

namespace Ydb::Scripting {

class ExecuteYqlRequest;

}

namespace Ydb::Query {

class ExecuteQueryRequest;
class ExecuteScriptRequest;

}

namespace Ydb::Tablet {

class ExecuteTabletMiniKQLRequest;
class ChangeTabletSchemaRequest;

}

namespace NKikimr::NGRpcService {

class IAuditCtx;

// RPC requests audit info collection methods.
//
// AuditContext{Start,Append,End}() methods store collected data into request context objects.
// AuditContextAppend() specializations extract specific info from request (and result) protos.
//

void AuditContextStart(IAuditCtx* ctx, const TString& database, const TString& userSID, const std::vector<std::pair<TString, TString>>& databaseAttrs);
void AuditContextEnd(IAuditCtx* ctx);

template <class TProtoRequest>
void AuditContextAppend(IAuditCtx* /*ctx*/, const TProtoRequest& /*request*/) {
    // do nothing by default
}

template <class TProtoRequest, class TProtoResult>
void AuditContextAppend(IAuditCtx* /*ctx*/, const TProtoRequest& /*request*/, const TProtoResult& /*result*/) {
    // do nothing by default
}

// ExecuteDataQuery
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request);
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request, const Ydb::Table::ExecuteQueryResult& result);

// PrepareDataQuery
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request);
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request, const Ydb::Table::PrepareQueryResult& result);

// BeginTransaction
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::BeginTransactionRequest& request, const Ydb::Table::BeginTransactionResult& result);

// CommitTransaction
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::CommitTransactionRequest& request);

// RollbackTransaction
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::RollbackTransactionRequest& request);

// BulkUpsert
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::BulkUpsertRequest& request);

// ExecuteYqlScript, StreamExecuteYqlScript
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Scripting::ExecuteYqlRequest& request);

// ExecuteQuery
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Query::ExecuteQueryRequest& request);

// ExecuteSrcipt
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Query::ExecuteScriptRequest& request);

// TabletService
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Tablet::ExecuteTabletMiniKQLRequest& request);
template <> void AuditContextAppend(IAuditCtx* ctx, const Ydb::Tablet::ChangeTabletSchemaRequest& request);

} // namespace NKikimr::NGRpcService
