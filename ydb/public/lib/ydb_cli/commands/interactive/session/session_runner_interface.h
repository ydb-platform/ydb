#pragma once

#include <util/generic/string.h>

#include <memory>
#include <unordered_map>

namespace NYdb::NConsoleClient {

struct TSessionSettings {
    TString Prompt;
    TString HistoryFilePath;
    TString HelpMessage;
    std::unordered_map<char, std::function<void()>> KeyHandlers;
};

class ISessionRunner {
public:
    using TPtr = std::unique_ptr<ISessionRunner>;

    virtual ~ISessionRunner() = default;

    virtual TSessionSettings GetSettings() const = 0;

    virtual void HandleLine(const TString& line) = 0;
};

} // namespace NYdb::NConsoleClient
