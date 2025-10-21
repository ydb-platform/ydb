#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/local_table_client.h>

namespace NFq {

struct TLocalYdbConnection : public IYdbConnection {

    TLocalYdbConnection(const TString& db, const TString& tablePathPrefix)
    : TablePathPrefix(tablePathPrefix)
    , Db(db)
    , TableClient(CreateLocalTableClient()) {
    }

    IYdbTableClient::TPtr GetTableClient() override {
        return TableClient;
    }
    TString GetTablePathPrefix() override {
        return Db + '/' + TablePathPrefix;
    }
    TString GetDb() override {
        return Db;
    }

    TString GetTablePathPrefixWithoutDb() override {
        return TablePathPrefix;
    }

private:
    const TString TablePathPrefix;
    const TString Db;
    IYdbTableClient::TPtr TableClient;
};

IYdbConnection::TPtr CreateLocalYdbConnection(const TString& db, const TString& tablePathPrefix) {
    return MakeIntrusive<TLocalYdbConnection>(db, tablePathPrefix);
}

} // namespace NFq
