#pragma once

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/generation_context.h>

namespace NFq {

struct TGenerationContext;
using TGenerationContextPtr = TIntrusivePtr<TGenerationContext>;

// struct IYdbTableGateway : public TThrRefBase {
//     using TPtr = TIntrusivePtr<IYdbTableGateway>;

//     virtual NYdb::TAsyncStatus RetryOperation(NYdb::NTable::TTableClient::TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) = 0;
//     //virtual NThreading::TFuture<NYdb::TStatus> RegisterCheckGeneration(const TGenerationContextPtr& context);
//     virtual NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(const TGenerationContextPtr& context, const TString& sql, NYdb::TParamsBuilder* params, NYdb::NTable::TTxControl txControl) = 0;

// };


// struct YdbSdkTableGateway : public IYdbTableGateway {

//     NYdb::NTable::TTableClient TableClient;

//    // YdbSdkTableGateway() {}

//     YdbSdkTableGateway(NYdb::NTable::TTableClient tableClient)
//         : TableClient(tableClient) {

//     }

//     NYdb::TAsyncStatus RetryOperation(NYdb::NTable::TTableClient::TOperationFunc&& operation, const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings())  override {
       
//         auto future = TableClient.RetryOperation(
//             operation, settings);

//         return future;
//       //  return StatusToIssues(future);
//     }

//     NThreading::TFuture<NYdb::NTable::TDataQueryResult> ExecuteDataQuery(const TGenerationContextPtr& context, const TString& sql, NYdb::TParamsBuilder* params, NYdb::NTable::TTxControl txControl) override {
//         return context->Session.ExecuteDataQuery(sql, txControl, params, /*context->ExecDataQuerySettings TODO */ );
//     }

//     // };
// };

} // namespace NFq
