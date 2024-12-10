#pragma once

#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb {

namespace NQuery {
    class TExecuteQueryIterator;
    class TQueryClient;
} // namespace NQuery

namespace NDebug {
    class TDebugClient;
};

namespace NConsoleClient {

class TCommandPing : public TYdbCommand, public TCommandWithFormat,
    public TInterruptibleCommand
{
public:
    enum class EPingKind {
        PlainGrpc = 0,
        GrpcProxy,
        PlainKqp,
        Select1,
        SchemeCache,
        TxProxy,
    };

public:
    TCommandPing();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int RunCommand(TConfig& config);
    int PrintResponse(NQuery::TExecuteQueryIterator& result);

    bool PingPlainGrpc(NDebug::TDebugClient& client);
    bool PingPlainKqp(NDebug::TDebugClient& client);
    bool PingGrpcProxy(NDebug::TDebugClient& client);
    bool PingSchemeCache(NDebug::TDebugClient& client);
    bool PingTxProxy(NDebug::TDebugClient& client);

    bool PingKqpSelect1(NQuery::TQueryClient& client, const TString& query);

private:
    int Count;
    int IntervalMs;

    EPingKind PingKind;
};

} // NYdb::NConsoleClient
} // namespace NYdb
