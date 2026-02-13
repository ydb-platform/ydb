#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <variant>
#include <vector>

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

    virtual void Finish(bool clear = false) = 0;

    // Runtime configuration
    virtual void SetHintsEnabled(bool enabled) = 0;
    virtual bool IsHintsEnabled() const = 0;

    virtual void SetColorSchema(const TColorSchema& schema) = 0;
    virtual TColorSchema GetColorSchema() const = 0;

    virtual void SetPrompt(const TString& prompt) = 0;

    virtual ~ILineReader() = default;
};

struct TLineReaderSettings {
    TDriver Driver;
    TString Database;
    TString Prompt;
    std::optional<TString> HistoryFilePath;
    std::vector<TString> AdditionalCommands;
    TString Placeholder;
    bool EnableYqlCompletion = true;
    bool EnableSwitchMode = true;
    bool ContinueAfterCancel = true;
};

ILineReader::TPtr CreateLineReader(const TLineReaderSettings& settings);

} // namespace NYdb::NConsoleClient
