#include "temp_tables.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NKqp {

THashMap<TString, TKqpTempTablesState::TTempTableInfo>::const_iterator
TKqpTempTablesState::FindInfo(const std::string_view& path, bool withSessionId) const {
    if (!withSessionId) {
        return TempTables.find(path);
    }

    const auto temporaryStoragePrefix = CanonizePath(GetSessionDirPath(Database, SessionId)) + "/";

    if (path.size() < temporaryStoragePrefix.size()) {
        return TempTables.end();
    }

    if (path.substr(0, temporaryStoragePrefix.size()) != temporaryStoragePrefix) {
        return TempTables.end();
    }

    return TempTables.find(path.substr(temporaryStoragePrefix.size() - 1));
}

TString GetSessionDirsBasePath(const TString& database) {
    return CanonizePath(JoinPath({database, ".tmp", "sessions"}));
}

TString GetSessionDirPath(const TString& database, const TString& sessionId) {
    return CanonizePath(JoinPath({database, ".tmp", "sessions", sessionId}));
}

TString GetTempTablePath(const TString& database, const TString& sessionId, const TString tablePath) {
    return CanonizePath(JoinPath({database, ".tmp", "sessions", sessionId, tablePath}));
}

TString GetCreateTempTablePath(const TString& database, const TString& sessionId, const TString tablePath) {
    return CanonizePath(JoinPath({".tmp", "sessions", sessionId, database, tablePath}));
}

} // namespace NKikimr::NKqp
