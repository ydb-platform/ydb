#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/logger/backend.h>

namespace NYdb {
namespace NConsoleClient {

class TLeafCommand : public TClientCommand {
public:
    using TClientCommand::TClientCommand;

    bool Prompt(TConfig& config) override;
};

class TYdbCommand : public TLeafCommand {
public:
    TYdbCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );

    static TDriver CreateDriver(TConfig& config);
    static TDriver CreateDriver(TConfig& config, std::unique_ptr<TLogBackend>&& loggingBackend);

private:
    static TDriverConfig CreateDriverConfig(TConfig& config);
};

class TYdbReadOnlyCommand : public TYdbCommand {
public:
    using TYdbCommand::TYdbCommand;

    bool Prompt(TConfig& config) override;
};

class TYdbSimpleCommand : public TYdbCommand {
public:
    TYdbSimpleCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );
    virtual void Config(TConfig& config) override;

protected:
    template<typename TSettingsType>
    TSettingsType&& FillSettings(TSettingsType&& settings) {
        if (ClientTimeout) {
            settings.ClientTimeout(TDuration::MilliSeconds(FromString<ui64>(ClientTimeout)));
        }
        return std::forward<TSettingsType>(settings);
    }

    TString ClientTimeout;
};

class TYdbOperationCommand : public TYdbCommand {
public:
    TYdbOperationCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    );
    virtual void Config(TConfig& config) override;

protected:
    template<typename TSettingsType>
    TSettingsType&& FillSettings(TSettingsType&& settings) {
        if (OperationTimeout) {
            ui64 operationTimeout = FromString<ui64>(OperationTimeout);
            settings.OperationTimeout(TDuration::MilliSeconds(operationTimeout));
            settings.ClientTimeout(TDuration::MilliSeconds(operationTimeout + 200));
        }
        return std::forward<TSettingsType>(settings);
    }

    TString OperationTimeout;
};

}
}
