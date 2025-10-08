#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/local_session.h>

namespace NFq {



struct TLocalYdbTableClient : public IYdbTableClient {

    NYdb::TAsyncStatus RetryOperation(
        TOperationFunc&& operation,
        const NYdb::NRetry::TRetryOperationSettings& /*settings*/ = NYdb::NRetry::TRetryOperationSettings()) override {

            auto session = MakeIntrusive<TLocalSession>();
            auto future = operation(session);
            return future.Apply([](const NYdb::TAsyncStatus& f){
                Cerr << "operation end" << Endl;
                return f;
            });
            //return future;//NThreading::MakeFuture<NYdb::TStatus>(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
    }
};

IYdbTableClient::TPtr CreateLocalTableClient() {
    return MakeIntrusive<TLocalYdbTableClient>();
}


//using IYdbConnectionPtr = TIntrusivePtr<IYdbTableClient>;



} // namespace NFq
