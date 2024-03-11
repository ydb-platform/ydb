#include "temp_tables.h"

namespace NKikimr::NKqp {

THashMap<TString, TKqpTempTablesState::TTempTableInfo>::const_iterator
TKqpTempTablesState::FindInfo(const std::string_view& path, bool withSessionId) const {
    if (!withSessionId) {
        return TempTables.find(path);
    }

    if (path.size() < SessionId.size()) {
        return TempTables.end();
    }
    size_t pos = path.size() - SessionId.size();
    if (path.substr(pos) != SessionId) {
        return TempTables.end();
    }

    return TempTables.find(path.substr(0, pos));
}

} // namespace NKikimr::NKqp
