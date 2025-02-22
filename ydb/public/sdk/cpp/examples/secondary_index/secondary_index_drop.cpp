#include "secondary_index.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

////////////////////////////////////////////////////////////////////////////////

static void DropTable(TTableClient& client, const std::string& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return session.DropTable(path).ExtractValueSync();
    }));
}

int RunDropTables(TDriver& driver, const std::string& prefix, int argc, char**) {
    if (argc > 1) {
        std::cerr << "Unexpected arguments after drop_tables" << std::endl;
        return 1;
    }

    TTableClient client(driver);
    DropTable(client, JoinPath(prefix, TABLE_SERIES));
    DropTable(client, JoinPath(prefix, TABLE_SERIES_REV_VIEWS));
    return 0;
}
