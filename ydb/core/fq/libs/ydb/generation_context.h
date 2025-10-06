#pragma once

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
#include <ydb/core/fq/libs/ydb/ydb_gateway.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

struct IYdbTableGateway;
//struct TYdbTableGatewayPtr;

//struct IYdbTableGateway : public TThrRefBase {
using TYdbTableGatewayPtr = TIntrusivePtr<IYdbTableGateway>;

struct TGenerationContext : public TThrRefBase {
    enum EOperationType {
        Register,
        RegisterCheck,
        Check,
    };

    // set internally, do not touch it
    EOperationType OperationType = Register;

    // within this session we execute transaction
    NYdb::NTable::TSession Session;

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

    TYdbTableGatewayPtr YdbGateway;
    NYdb::NTable::TExecDataQuerySettings ExecDataQuerySettings;

    TGenerationContext(NYdb::NTable::TSession session,
                       bool commitTx,
                       const TString& tablePathPrefix,
                       const TString& table,
                       const TString& primaryKeyColumn,
                       const TString& generationColumn,
                       const TString& primaryKey,
                       ui64 generation,
                       TYdbTableGatewayPtr ydbGateway,
                       const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {})
        : Session(session)
        , CommitTx(commitTx)
        , TablePathPrefix(tablePathPrefix)
        , Table(table)
        , PrimaryKeyColumn(primaryKeyColumn)
        , GenerationColumn(generationColumn)
        , PrimaryKey(primaryKey)
        , Generation(generation)
        , YdbGateway(ydbGateway)
        , ExecDataQuerySettings(execDataQuerySettings)
    {
    }
};

using TGenerationContextPtr = TIntrusivePtr<TGenerationContext>;



struct IYdbTableGateway : public TThrRefBase {
    using TPtr = TIntrusivePtr<IYdbTableGateway>;

    virtual NYdb::TAsyncStatus RetryOperation(NYdb::NTable::TTableClient::TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) = 0;
    //virtual NThreading::TFuture<NYdb::TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context);
    virtual NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(const TGenerationContextPtr& context, const TString& sql, NYdb::TParamsBuilder* params, NYdb::NTable::TTxControl txControl, const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) = 0;
    virtual TString GetDb() const = 0;
    virtual TString GetTablePathPrefix() const = 0;
};

using TYdbTableGatewayPtr = IYdbTableGateway::TPtr;


struct YdbSdkTableGateway : public IYdbTableGateway {

    NYdb::NTable::TTableClient TableClient;
    const TString DB;
    const TString TablePathPrefix;

   // YdbSdkTableGateway() {}

    YdbSdkTableGateway(NYdb::NTable::TTableClient tableClient, const TString& db, const TString& tablePathPrefix)
        : TableClient(tableClient)
        , DB(db)
        , TablePathPrefix(tablePathPrefix) {

    }

    NYdb::TAsyncStatus RetryOperation(NYdb::NTable::TTableClient::TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings())  override {
       
        auto future = TableClient.RetryOperation(
            operation, settings);

        return future;
      //  return StatusToIssues(future);
    }

    NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(const TGenerationContextPtr& context, const TString& sql, NYdb::TParamsBuilder* params, NYdb::NTable::TTxControl txControl, const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings = {}) override {
        return context->Session.ExecuteDataQuery(sql, txControl, params->Build(), execDataQuerySettings);
    }

    TString GetDb() const override {
        return DB;
    };

    TString GetTablePathPrefix() const override {
        return TablePathPrefix;
    };

};


} // namespace NFq

// template<>
// void Out<NFq::TGenerationContext::EOperationType>(
//     IOutputStream& out,
//     const NFq::TCoordinatorId& coordinatorId)
// {
//     coordinatorId.PrintTo(out);
// }
