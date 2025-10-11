
#include <ydb/core/fq/libs/ydb/ydb_local_connection.h>
#include <ydb/core/fq/libs/ydb/local_table_client.h>

namespace NFq {


struct TLocalYdbConnection : public IYdbConnection {

    TLocalYdbConnection(const TString& db, const TString& tablePathPrefix)
    : TablePathPrefix(tablePathPrefix)
    , Db(db) {
    }

    IYdbTableClient::TPtr GetYdbTableClient() override {
        return CreateLocalTableClient();  // TODO member
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
};

IYdbConnection::TPtr CreateLocalYdbConnection(const TString& db, const TString& tablePathPrefix) {
    return MakeIntrusive<TLocalYdbConnection>(db, tablePathPrefix);
}

} // namespace NFq
