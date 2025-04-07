#include "secondary_index.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

////////////////////////////////////////////////////////////////////////////////

static void CreateSeriesTable(TTableClient& client, const std::string& prefix) {
    ThrowOnError(client.RetryOperationSync([prefix](TSession session) {
        auto desc = TTableBuilder()
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("series_info", EPrimitiveType::Utf8)
            .AddNullableColumn("release_date", EPrimitiveType::Uint32)
            .AddNullableColumn("views", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("series_id")
            .Build();

        return session.CreateTable(JoinPath(prefix, TABLE_SERIES), std::move(desc)).ExtractValueSync();
    }));
}

static void CreateSeriesIndexTable(TTableClient& client, const std::string& prefix) {
    ThrowOnError(client.RetryOperationSync([prefix](TSession session) {
        auto desc = TTableBuilder()
            .AddNullableColumn("rev_views", EPrimitiveType::Uint64)
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumns({ "rev_views", "series_id" })
            .Build();

        return session.CreateTable(JoinPath(prefix, TABLE_SERIES_REV_VIEWS), std::move(desc)).ExtractValueSync();
    }));
}

int RunCreateTables(TDriver& driver, const std::string& prefix, int argc, char**) {
    if (argc > 1) {
        std::cerr << "Unexpected arguments after create_tables" << std::endl;
        return 1;
    }

    TTableClient client(driver);
    CreateSeriesTable(client, prefix);
    CreateSeriesIndexTable(client, prefix);
    return 0;
}
