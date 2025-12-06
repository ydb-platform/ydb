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
    bool EnableYqlCompletion = true;
};

class ILineReaderController {
public:
    using TPtr = std::shared_ptr<ILineReaderController>;

    virtual ~ILineReaderController() = default;

    virtual void Setup(const TSessionSettings& settings) = 0;
};

class ISessionRunner {
public:
    using TPtr = std::shared_ptr<ISessionRunner>;

    virtual ~ISessionRunner() = default;

    virtual bool Setup(ILineReaderController::TPtr controller) = 0;

    virtual void HandleLine(const TString& line) = 0;
};

} // namespace NYdb::NConsoleClient
