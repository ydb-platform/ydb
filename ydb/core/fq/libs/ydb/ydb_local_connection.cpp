
#include <ydb/core/fq/libs/ydb/ydb_local_connection.h>
#include <ydb/core/fq/libs/ydb/local_table_client.h>

namespace NFq {


struct TLocalYdbConnection : public IYdbConnection {

    TLocalYdbConnection(const TString& tablePathPrefix)
    : TablePathPrefix(tablePathPrefix) {
    }

    IYdbTableClient::TPtr GetYdbTableClient() override {
        return CreateLocalTableClient();
    }
    TString GetTablePathPrefix() override {
        return TablePathPrefix;
    }
    TString GetDb() override {
        return "";
    }

    const TString TablePathPrefix;
};

IYdbConnection::TPtr CreateLocalYdbConnection(const TString& tablePathPrefix) {
    return MakeIntrusive<TLocalYdbConnection>(tablePathPrefix);
}

} // namespace NFq
