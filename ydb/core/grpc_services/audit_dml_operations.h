#pragma once
#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NGRpcService {

class IRequestCtxBase;
class IRequestCtx;

// RPC requests audit info collection methods.
//
// AuditContext{Start,Append,End}() methods store collected data into request context objects.
// AuditContextAppend() specializations extract specific info from request (and result) protos.
//

void AuditContextStart(IRequestCtxBase* ctx, const TString& database, const TString& userSID);
void AuditContextEnd(IRequestCtxBase* ctx);

template <class TProtoRequest>
void AuditContextAppend(IRequestCtx* /*ctx*/, const TProtoRequest& /*request*/) {
    // do nothing by default
}

template <class TProtoRequest, class TProtoResult>
void AuditContextAppend(IRequestCtx* /*ctx*/, const TProtoRequest& /*request*/, const TProtoResult& /*result*/) {
    // do nothing by default
}

// ExecuteDataQuery
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request);
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request, const Ydb::Table::ExecuteQueryResult& result);

// PrepareDataQuery
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request);
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request, const Ydb::Table::PrepareQueryResult& result);

// BeginTransaction
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::BeginTransactionRequest& request, const Ydb::Table::BeginTransactionResult& result);

// CommitTransaction
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::CommitTransactionRequest& request);

// RollbackTransaction
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::RollbackTransactionRequest& request);

// BulkUpsert
template <> void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::BulkUpsertRequest& request);

} // namespace NKikimr::NGRpcService
