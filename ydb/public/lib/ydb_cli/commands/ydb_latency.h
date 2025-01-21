#pragma once

#include "ydb_command.h"

#include "ydb_ping.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb::NConsoleClient {

class TCommandLatency
    : public TYdbCommand
    , public TCommandWithFormat
    , public TInterruptibleCommand
{
public:
    enum class EFormat {
        Plain = 0,
        CSV,
    };

public:
    TCommandLatency();

    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int IntervalSeconds;
    int MaxInflight;
    EFormat Format;
    TCommandPing::EPingKind RunKind;
    double Percentile;
};

} // NYdb::NConsoleClient
