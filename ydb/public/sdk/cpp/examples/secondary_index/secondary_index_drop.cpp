#include "secondary_index.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;

////////////////////////////////////////////////////////////////////////////////

static void DropTable(TTableClient& client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return session.DropTable(path).ExtractValueSync();
    }));
}

int RunDropTables(TDriver& driver, const TString& prefix, int argc, char**) {
    if (argc > 1) {
        Cerr << "Unexpected arguments after drop_tables" << Endl;
        return 1;
    }

    TTableClient client(driver);
    DropTable(client, JoinPath(prefix, TABLE_SERIES));
    DropTable(client, JoinPath(prefix, TABLE_SERIES_REV_VIEWS));
    return 0;
}
