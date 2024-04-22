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

namespace {
    const TString TmpDirectoryName = ".tmp";
    const TString SessionsDirectoryName = "sessions";
}

TString GetSessionDirsBasePath(const TString& database) {
    return CanonizePath(JoinPath({database, TmpDirectoryName, SessionsDirectoryName}));
}

TString GetSessionDirPath(const TString& database, const TString& sessionId) {
    return CanonizePath(JoinPath({database, TmpDirectoryName, SessionsDirectoryName, sessionId}));
}

TString GetTempTablePath(const TString& database, const TString& sessionId, const TString tablePath) {
    return CanonizePath(JoinPath({database, TmpDirectoryName, SessionsDirectoryName, sessionId, tablePath}));
}

TString GetCreateTempTablePath(const TString& database, const TString& sessionId, const TString tablePath) {
    return CanonizePath(JoinPath({TmpDirectoryName, SessionsDirectoryName, sessionId, database, tablePath}));
}

bool IsSessionsDirPath(const TStringBuf database, const TStringBuf path) {
    return path.StartsWith(TmpDirectoryName)
        || (path.size() > database.size() + 1 + TmpDirectoryName.size()
            && path.StartsWith(database)
            && path.SubString(database.size() + 1, TmpDirectoryName.size()) == TmpDirectoryName);
}

bool IsSessionsDirPath(const TStringBuf database, const TString& workingDir, const TString& name) {
    const auto joinedPath = JoinPath({workingDir, name});
    return IsSessionsDirPath(database, joinedPath);
}

} // namespace NKikimr::NKqp
