#pragma once

#include "interactive_log.h"

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class ILineReader {
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
    TString Prompt;
    std::optional<TString> HistoryFilePath;
    std::optional<TString> HelpMessage;
    std::vector<TString> AdditionalCommands;
    bool EnableYqlCompletion = true;
    bool EnableSwitchMode = true;
    bool ContinueAfterCancel = true;
};

ILineReader::TPtr CreateLineReader(const TLineReaderSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
