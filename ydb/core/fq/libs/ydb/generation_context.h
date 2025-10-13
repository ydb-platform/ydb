#pragma once

#include <memory>
#include <mutex>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/core/fq/libs/config/protos/storage.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <ydb/core/protos/config.pb.h>
#include <functional>
#include <ydb/core/fq/libs/ydb/session.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

//struct IYdbTableGateway;
//struct TYdbTableGatewayPtr;

//struct IYdbTableGateway : public TThrRefBase {
//using TYdbTableGatewayPtr = TIntrusivePtr<IYdbTableGateway>;

struct TGenerationContext : public TThrRefBase {
    enum EOperationType {
        Register,
        RegisterCheck,
        Check,
    };

    // set internally, do not touch it
    EOperationType OperationType = Register;

    // within this session we execute transaction
    ISession::TPtr Session = nullptr;

    // - In Register and RegisterCheck operation - whether
    // to commit or not after upserting new generation (usually true)
    // - In Check operation - whether to commit or not
    // after selecting generation (normally false)
    //
    // - When check succeeds, and flag is true, it's up to caller
    // to finish transaction.
    // - When check fails then transaction is aborted automatically
    const bool CommitTx;

    const TString TablePathPrefix;
    const TString Table;
    const TString PrimaryKeyColumn;
    const TString GenerationColumn;

    const TString PrimaryKey;

    // - In Register operation - it is new generation to be registered,
    // caller with higher generation wins
    // - In RegisterCheck operation - it is new generation to be registered,
    // if DB contains smaller generation, when generation is the same - operation
    // equals to Check
    // - In Check operation - expected generation, caller normally uses
    // it with Transaction (must have CommitTx = false)
    const ui64 Generation;

    std::optional<NYdb::NTable::TTransaction> Transaction;

    // result of Select
    ui64 GenerationRead = 0;

    //TYdbTableGatewayPtr YdbGateway;
    NYdb::NTable::TExecDataQuerySettings ExecDataQuerySettings;

    TGenerationContext(ISession::TPtr session,
                       bool commitTx,
                       const TString& tablePathPrefix,
                       const TString& table,
                       const TString& primaryKeyColumn,
                       const TString& generationColumn,
                       const TString& primaryKey,
                       ui64 generation,
                       const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {})
        : Session(std::move(session))
        , CommitTx(commitTx)
        , TablePathPrefix(tablePathPrefix)
        , Table(table)
        , PrimaryKeyColumn(primaryKeyColumn)
        , GenerationColumn(generationColumn)
        , PrimaryKey(primaryKey)
        , Generation(generation)
        , ExecDataQuerySettings(execDataQuerySettings)
    {
    }
};

using TGenerationContextPtr = TIntrusivePtr<TGenerationContext>;

// struct TRetryContext {
//     TMaybe<NYdb::NTable::TSession> Session;
//     NYdb::NTable::TSession GetSession() {
//         return *Session;
//     }
// };



// struct IYdbTableGateway : public TThrRefBase {
//     using TPtr = TIntrusivePtr<IYdbTableGateway>;

//     virtual NYdb::TAsyncStatus RetryOperation(TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) = 0;
//     //virtual NThreading::TFuture<NYdb::TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context);
//     virtual NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
//         const TGenerationContextPtr& context,
//         const TString& sql,
//         std::shared_ptr<NYdb::TParamsBuilder> params,
//         NYdb::NTable::TTxControl txControl,
//         const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) = 0;


//     virtual TString GetDb() const = 0;
//     virtual TString GetTablePathPrefix() const = 0;
// };

// using TYdbTableGatewayPtr = IYdbTableGateway::TPtr;


// struct YdbSdkTableGateway : public IYdbTableGateway {

//     NYdb::NTable::TTableClient TableClient;
//     const TString DB;
//     const TString TablePathPrefix;

//    // YdbSdkTableGateway() {}

//     YdbSdkTableGateway(NYdb::NTable::TTableClient tableClient, const TString& db, const TString& tablePathPrefix)
//         : TableClient(tableClient)
//         , DB(db)
//         , TablePathPrefix(tablePathPrefix) {

//     }

//     NYdb::TAsyncStatus RetryOperation(TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings())  override {
//         auto future = TableClient.RetryOperation([op = std::move(operation)](NYdb::NTable::TSession session) {
//             return op(TRetryContext{session});
//         }, settings);

//         return future;
//     }

//     NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
//         const TGenerationContextPtr& context,
//         const TString& sql,
//         std::shared_ptr<NYdb::TParamsBuilder> params,
//         NYdb::NTable::TTxControl txControl,
//         const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) override {
//         return context->Session->ExecuteDataQuery(sql, txControl, params->Build(), execDataQuerySettings);
//     }

//     TString GetDb() const override {
//         return DB;
//     };

//     TString GetTablePathPrefix() const override {
//         return TablePathPrefix;
//     };
// };

// struct YdbLocalTableGateway : public IYdbTableGateway {
//     const TString DB;
//     const TString TablePathPrefix;

//     YdbLocalTableGateway(const TString& db, const TString& tablePathPrefix)
//         : DB(db)
//         , TablePathPrefix(tablePathPrefix) {
//     }

//     NYdb::TAsyncStatus RetryOperation(TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& /*settings*/ = NYdb::NRetry::TRetryOperationSettings())  override {
        
//         Cerr << "RetryOperation" << Endl;
//         return operation(TRetryContext{});
//     }

//     NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(
//         const TGenerationContextPtr& /*context*/,
//         const TString& sql,
//         std::shared_ptr<NYdb::TParamsBuilder> params,
//         NYdb::NTable::TTxControl txControl,
//         const NYdb::NTable::TExecDataQuerySettings& /*execDataQuerySettings*/ = {}) override {
//         Cerr << "ExecuteDataQuery" << Endl;

//         auto* session = MakeSession();
//         return session->ExecuteDataQuery(sql, params, txControl);
//         //NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise = NThreading::NewPromise<NYdb::NTable::TDataQueryResult>();
//         //NActors::TActivationContext::AsActorContext().Register(NewDataQuery(promise, sql, params, txControl).release());
//         //return promise.GetFuture();
//     }

//     TString GetDb() const override {
//         return DB;
//     };

//     TString GetTablePathPrefix() const override {
//         return TablePathPrefix;
//     };
// };


} // namespace NFq

// template<>
// void Out<NFq::TGenerationContext::EOperationType>(
//     IOutputStream& out,
//     const NFq::TCoordinatorId& coordinatorId)
// {
//     coordinatorId.PrintTo(out);
// }
