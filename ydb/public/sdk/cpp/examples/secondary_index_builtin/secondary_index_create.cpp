#include "secondary_index.h"

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NLastGetopt;

static void CreateSeriesTable(TTableClient& client, const TString& path) {

     ThrowOnError(client.RetryOperationSync([path](TSession session) {

        auto desc = TTableBuilder()
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("info", EPrimitiveType::Utf8)
            .AddNullableColumn("release_date", EPrimitiveType::Datetime)
            .AddNullableColumn("views", EPrimitiveType::Uint64)
            .AddNullableColumn("uploaded_user_id", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("series_id")
            .AddSecondaryIndex("views_index", "views")
            .AddSecondaryIndex("users_index", "uploaded_user_id")
            .Build();

        return session.CreateTable(JoinPath(path, TABLE_SERIES), std::move(desc)).GetValueSync();
    }));

    ThrowOnError(client.RetryOperationSync([path] (TSession session) {

       auto desc = TTableBuilder()
            .AddNullableColumn("user_id", EPrimitiveType::Uint64)
            .AddNullableColumn("name", EPrimitiveType::Utf8)
            .AddNullableColumn("age", EPrimitiveType::Uint32)
            .SetPrimaryKeyColumn("user_id")
            .AddSecondaryIndex("name_index", "name")
            .Build();

        return session.CreateTable(JoinPath(path, TABLE_USERS), std::move(desc)).GetValueSync();
    }));
}

int Create(TDriver& driver, const TString& path) {
    TTableClient client(driver);
    CreateSeriesTable(client, path);
    return 0;
}

