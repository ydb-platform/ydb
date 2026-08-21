#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>

namespace NYdb::NConsoleClient {

class TCommandSession : public TClientCommandTree {
public:
    TCommandSession();
};

class TCommandListSessions : public TYdbSimpleCommand,
                             public TCommandWithOutput {
public:
    TCommandListSessions();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString State;
    TString User;
    TString Application;
    TString ResourcePool;
    TMaybe<ui32> NodeId;
    TMaybe<TDuration> OlderThan;
    TMaybe<TDuration> QueryRunningFor;
    ui64 Limit = 1000;
};

class TCommandWithSessionId : public TYdbSimpleCommand {
public:
    using TYdbSimpleCommand::TYdbSimpleCommand;

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;

protected:
    TString SessionId;
};

class TCommandGetSession : public TCommandWithSessionId,
                           public TCommandWithOutput {
public:
    TCommandGetSession();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandTerminateSession : public TCommandWithSessionId {
public:
    TCommandTerminateSession();

    int Run(TConfig& config) override;
};

} // namespace NYdb::NConsoleClient
