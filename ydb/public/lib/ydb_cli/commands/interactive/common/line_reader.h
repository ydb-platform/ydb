#pragma once

#include "interactive_log.h"

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class ILineReaderController {
public:
    using TPtr = std::shared_ptr<ILineReaderController>;

    struct TSettings {
        TString Prompt;
        std::optional<TString> HistoryFilePath;
        std::optional<TString> HelpMessage;
        std::unordered_map<char, std::function<void()>> KeyHandlers;
        bool EnableYqlCompletion = true;
        bool EnableSwitchMode = true;
    };

    virtual ~ILineReaderController() = default;

    virtual void Setup(const TSettings& settings) = 0;
};

class ILineReader : public ILineReaderController {
public:
    using TPtr = std::shared_ptr<ILineReader>;

    struct TLine {
        TString Data;
    };

    struct TSwitch {
    };

    virtual std::optional<std::variant<TLine, TSwitch>> ReadLine(const TString& defaultValue = "") = 0;

    virtual void Finish() = 0;

    virtual ~ILineReader() = default;
};

struct TLineReaderSettings {
    TDriver Driver;
    TString Database;
    bool ContinueAfterCancel = true;
};

ILineReader::TPtr CreateLineReader(const TLineReaderSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
