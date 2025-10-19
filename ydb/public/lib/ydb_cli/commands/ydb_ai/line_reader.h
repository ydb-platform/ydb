#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/string.h>
#include <util/system/file_lock.h>

#include <optional>

namespace NYdb::NConsoleClient::NAi {

class TLineReader {
public:
    TLineReader(TString prompt, TString historyFilePath);

    std::optional<TString> ReadLine();

private:
    TTryGuard<TFileLock> TryLockHistory();

    void AddToHistory(const TString& line);

private:
    TString Prompt;
    TString HistoryFilePath;
    TFileLock HistoryFileLock;
    replxx::Replxx Rx;
};

} // namespace NYdb::NConsoleClient::NAi
