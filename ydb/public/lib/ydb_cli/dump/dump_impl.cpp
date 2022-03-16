#include "dump_impl.h"

#include <ydb/public/lib/ydb_cli/dump/util/util.h>

namespace NYdb {
namespace NDump {

TDumpClient::TDumpClient(NScheme::TSchemeClient& schemeClient, NTable::TTableClient& tableClient)
    : SchemeClient(schemeClient)
    , TableClient(tableClient)
{
    Y_UNUSED(SchemeClient);
    Y_UNUSED(TableClient);
}

TDumpResult TDumpClient::Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings) {
    Y_UNUSED(dbPath);
    Y_UNUSED(TableClient);
    Y_UNUSED(fsPath);
    Y_UNUSED(settings);
    return Result<TDumpResult>(EStatus::CLIENT_CALL_UNIMPLEMENTED, "Not implemented");
}

} // NDump
} // NYdb
