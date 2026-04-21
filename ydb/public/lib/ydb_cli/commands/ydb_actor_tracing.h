#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

namespace NYdb::NConsoleClient {

class TCommandActorTracing : public TClientCommandTree {
public:
    TCommandActorTracing();
};

class TCommandActorTracingStart : public TYdbCommand {
public:
    TCommandActorTracingStart();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandActorTracingStop : public TYdbCommand {
public:
    TCommandActorTracingStop();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandActorTracingFetch : public TYdbCommand {
public:
    TCommandActorTracingFetch();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString OutputPath;
};

} // namespace NYdb::NConsoleClient
