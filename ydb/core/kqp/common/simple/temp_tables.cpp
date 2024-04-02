#include "temp_tables.h"

namespace NKikimr::NKqp {

THashMap<TString, TKqpTempTablesState::TTempTableInfo>::const_iterator
TKqpTempTablesState::FindInfo(const std::string_view& path, bool withSessionId) const {
    if (!withSessionId) {
        return TempTables.find(path);
    }

    const auto temporaryStoragePrefix = Database + "/.tmp/" + SessionId + "/";

    if (path.size() < temporaryStoragePrefix.size()) {
        return TempTables.end();
    }

    if (path.substr(0, temporaryStoragePrefix.size()) != temporaryStoragePrefix) {
        return TempTables.end();
    }

    return TempTables.find(path.substr(temporaryStoragePrefix.size() - 1));
}

} // namespace NKikimr::NKqp
