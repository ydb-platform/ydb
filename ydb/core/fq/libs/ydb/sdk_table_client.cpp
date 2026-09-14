#include <ydb/core/fq/libs/ydb/sdk_table_client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/table_client.h>
#include <ydb/core/fq/libs/ydb/sdk_session.h>

namespace NFq {

namespace {

struct TSdkYdbTableClient : public IYdbTableClient {

    TSdkYdbTableClient(
        const NYdb::TDriver& driver,
        const NYdb::NTable::TClientSettings& settings)
        : TableClient(driver, settings) {
    }

    NYdb::TAsyncStatus RetryOperation(TOperationFunc&& operation,
        const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) override {
        return TableClient.RetryOperation([operation](NYdb::NTable::TSession s) {
            auto session = CreateSdkSession(s);
            return operation(session);
        }, settings);
    }

private:
    NYdb::NTable::TTableClient TableClient;
};

} // namespace

IYdbTableClient::TPtr CreateSdkTableClient(
    const NYdb::TDriver& driver,
    const NYdb::NTable::TClientSettings& settings) {
    return MakeIntrusive<TSdkYdbTableClient>(driver, settings);
}

} // namespace NFq
