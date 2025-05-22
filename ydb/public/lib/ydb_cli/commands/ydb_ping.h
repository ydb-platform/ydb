#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb {

inline namespace Dev {
namespace NQuery {
    class TQueryClient;
    class TSession;
} // namespace NQuery

namespace NDebug {
    class TDebugClient;
    struct TActorChainPingSettings;
}
}

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
        ActorChain,
        AllKinds,
    };

public:
    TCommandPing();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

    static bool PingPlainGrpc(NDebug::TDebugClient& client);
    static bool PingPlainKqp(NDebug::TDebugClient& client);
    static bool PingGrpcProxy(NDebug::TDebugClient& client);
    static bool PingSchemeCache(NDebug::TDebugClient& client);
    static bool PingTxProxy(NDebug::TDebugClient& client);
    static bool PingActorChain(NDebug::TDebugClient& client, const NDebug::TActorChainPingSettings& settings);

    static bool PingKqpSelect1(NQuery::TQueryClient& client, const TString& query);
    static bool PingKqpSelect1(NQuery::TSession& session, const TString& query);

private:
    int RunCommand(TConfig& config);

private:
    int Count;
    int IntervalMs;

    EPingKind PingKind;
};

} // NYdb::NConsoleClient
} // namespace NYdb
