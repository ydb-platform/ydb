#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/local_table_client.h>
#include <ydb/core/fq/libs/ydb/util.h>

namespace NFq {

namespace {

struct TLocalYdbConnection : public IYdbConnection {

    TLocalYdbConnection(const TString& db, const TString& tablePathPrefix)
        : TablePathPrefix(tablePathPrefix)
        , Db(db)
        , TableClient(CreateLocalTableClient()) {
    }

    IYdbTableClient::TPtr GetTableClient() const override {
        return TableClient;
    }
    TString GetTablePathPrefix() const override {
        return JoinPath(Db, TablePathPrefix);
    }
    TString GetDb()const override {
        return Db;
    }

    TString GetTablePathPrefixWithoutDb() const override {
        return TablePathPrefix;
    }

private:
    const TString TablePathPrefix;
    const TString Db;
    IYdbTableClient::TPtr TableClient;
};

} // namespace

IYdbConnection::TPtr CreateLocalYdbConnection(const TString& db, const TString& tablePathPrefix) {
    return MakeIntrusive<TLocalYdbConnection>(db, tablePathPrefix);
}

} // namespace NFq
