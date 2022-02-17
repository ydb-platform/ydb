#include "secondary_index.h"

using namespace NYdb::NTable;
using namespace NYdb;

static void DropTable(TTableClient& client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path] (TSession session) {
        return session.DropTable(path).ExtractValueSync();
    }));
}

int Drop(NYdb::TDriver& driver, const TString& path) {

    TTableClient client(driver);
    DropTable(client, JoinPath(path, TABLE_SERIES));
    DropTable(client, JoinPath(path, TABLE_USERS));

    return 0;
}
