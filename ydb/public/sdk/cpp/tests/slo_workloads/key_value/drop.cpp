#include "key_value.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;

static void DropTable(TTableClient& client, const std::string& path) {
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return session.DropTable(path).ExtractValueSync();
    }));
}

int DropTable(TDatabaseOptions& dbOptions) {
    TTableClient client(dbOptions.Driver);
    DropTable(client, JoinPath(dbOptions.Prefix, TableName));
    Cout << "Table dropped." << Endl;
    return EXIT_SUCCESS;
}
