#include "ydb_ping.h"

#include <ydb/public/sdk/cpp/client/ydb_debug/client.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

#include <library/cpp/time_provider/monotonic.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr int DEFAULT_COUNT = 100;
constexpr int DEFAULT_INTERVAL_MS = 100;
constexpr TCommandPing::EPingType DEFAULT_PING_TYPE = TCommandPing::EPingType::Select1;

const TVector<TString> PingTypeDescriptions = {
    "ping returns right from the GRPC layer",
    "ping goes through GRPC layer right to the GRPC proxy and returns",
    "ping goes until query processing layer and returns without any query execution",
    "ping executes a very simple 'SELECT 1;' query",
    "ping goes through GRPC layer to SchemeCache and returns",
    "ping goes through GRPC layer to TxProxy and allocates TxId",
};

} // anonymous

using namespace NKikimr::NOperationId;

TCommandPing::TCommandPing()
    : TYdbCommand("ping", {}, "ping YDB")
    , Count(DEFAULT_COUNT)
    , IntervalMs(DEFAULT_INTERVAL_MS)
    , PingType(DEFAULT_PING_TYPE)
{}

void TCommandPing::Config(TConfig& config) {
    TYdbCommand::Config(config);

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    TStringStream pingTypesDescription;
    pingTypesDescription << "Ping types, available options:";
    for (size_t i = 0; i < PingTypeDescriptions.size(); ++i) {
        EPingType type = static_cast<EPingType>(i);

        pingTypesDescription << "\n" << colors.ItalicOn() << type << colors.ItalicOff()
            << "\n    " << PingTypeDescriptions[i];
    }
    pingTypesDescription << "\nDefault: " << PingType << Endl;


    config.Opts->AddLongOption(
        'c', "count", TStringBuilder() << "stop after <count> replies, default " << DEFAULT_COUNT)
            .RequiredArgument("COUNT").StoreResult(&Count);

    config.Opts->AddLongOption(
        'i', "interval", TStringBuilder() << "<interval> ms between pings, default " << DEFAULT_INTERVAL_MS)
            .RequiredArgument("INTERVAL").StoreResult(&IntervalMs);

    config.Opts->AddLongOption('t', "type", pingTypesDescription.Str())
        .OptionalArgument("STRING").StoreResult(&PingType);
}

void TCommandPing::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandPing::Run(TConfig& config) {
    return RunCommand(config);
}

int TCommandPing::RunCommand(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);
    NDebug::TDebugClient pingClient(driver);
    SetInterruptHandlers();

    std::vector<int> durations;
    durations.reserve(Count);
    size_t failedCount = 0;

    const TString query = "SELECT 1;";

    for (int i = 0; i < Count && !IsInterrupted(); ++i) {
        bool isOk;
        auto start = NMonotonic::TMonotonic::Now();

        switch (PingType) {
        case EPingType::PlainGrpc:
            isOk = PingPlainGrpc(pingClient);
            break;
        case EPingType::PlainKqp:
            isOk = PingPlainKqp(pingClient);
            break;
        case EPingType::GrpcProxy:
            isOk = PingGrpcProxy(pingClient);
            break;
        case EPingType::Select1:
            isOk = PingKqpSelect1(queryClient, query);
            break;
        case EPingType::SchemeCache:
            isOk = PingSchemeCache(pingClient);
            break;
        case EPingType::TxProxy:
            isOk = PingTxProxy(pingClient);
            break;
        default:
            std::cerr << "Unknown ping type" << std::endl;
            return EXIT_FAILURE;
        }

        auto end = NMonotonic::TMonotonic::Now();
        auto deltaUs = (end - start).MicroSeconds();

        std::cout << i << (isOk ? " ok" : " failed") << " in " << deltaUs << " us" << std::endl;

        if (!isOk) {
            ++failedCount;
        }

        durations.push_back(deltaUs);

        Sleep(TDuration::MilliSeconds(IntervalMs));
    }

    std::sort(durations.begin(), durations.end());

    std::cout << std::endl;
    std::cout << "Total: " << durations.size() << ", failed: " << failedCount << std::endl;
    const auto& percentiles = {0.5, 0.9, 0.95, 0.99};

    for (double percentile: percentiles) {
        size_t index = (size_t)(durations.size() * percentile);
        std::cout << (int)(percentile * 100) << "%: "
            << durations[index] << " us" << std::endl;
    }

    return 0;
}

bool TCommandPing::PingPlainGrpc(NDebug::TDebugClient& client) {
    auto asyncResult = client.PingPlainGrpc(NDebug::TPlainGrpcPingSettings());
    asyncResult.GetValueSync();

    return true;
}

bool TCommandPing::PingPlainKqp(NDebug::TDebugClient& client) {
    auto asyncResult = client.PingKqpProxy(NDebug::TKqpProxyPingSettings());
    auto result = asyncResult.GetValueSync();

    if (result.IsSuccess()) {
        return true;
    }

    return false;
}

bool TCommandPing::PingGrpcProxy(NDebug::TDebugClient& client) {
    auto asyncResult = client.PingGrpcProxy(NDebug::TGrpcProxyPingSettings());
    auto result = asyncResult.GetValueSync();

    if (result.IsSuccess()) {
        return true;
    }

    return false;
}

bool TCommandPing::PingSchemeCache(NDebug::TDebugClient& client) {
    auto asyncResult = client.PingSchemeCache(NDebug::TSchemeCachePingSettings());
    auto result = asyncResult.GetValueSync();

    if (result.IsSuccess()) {
        return true;
    }

    return false;
}

bool TCommandPing::PingTxProxy(NDebug::TDebugClient& client) {
    auto asyncResult = client.PingTxProxy(NDebug::TTxProxyPingSettings());
    auto result = asyncResult.GetValueSync();

    if (result.IsSuccess()) {
        return true;
    }

    return false;
}

bool TCommandPing::PingKqpSelect1(NQuery::TQueryClient& client, const TString& query) {
    // Single stream execution
    NQuery::TExecuteQuerySettings settings;

    // Execute query
    settings.ExecMode(NQuery::EExecMode::Execute);
    settings.StatsMode(NQuery::EStatsMode::None);

    settings.Syntax(NQuery::ESyntax::YqlV1);

    // Execute query without parameters
    auto asyncResult = client.StreamExecuteQuery(
        query,
        NQuery::TTxControl::NoTx(),
        settings
    );

    auto result = asyncResult.GetValueSync();
    while (!IsInterrupted()) {
        auto streamPart = result.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (streamPart.EOS()) {
                return false;
            }
            return false;
        }

        if (streamPart.HasResultSet()) {
            return true;
        }
    }

    return false;
}

} // NYdb::NConsoleClient
