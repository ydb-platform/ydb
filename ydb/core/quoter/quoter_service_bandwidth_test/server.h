#pragma once
#include "options.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/test_client.h>

#include <util/generic/maybe.h>

#include <atomic>

namespace NKikimr {

struct TRequestStats {
    std::atomic<size_t> RequestsCount = 0;
    std::atomic<size_t> ResponsesCount = 0;
    std::atomic<size_t> OkResponses = 0;
    std::atomic<size_t> DeadlineResponses = 0;
};

class TTestServer {
public:
    TTestServer(const TOptions& opts);

    void RunQuotaRequesters(TRequestStats& stats);

    static std::pair<TString, TString> GetKesusPathAndName(size_t i);
    static TString GetKesusPath(size_t i);
    static TString GetKesusResource(size_t i);
    static TString GetKesusRootResource();

private:
    void RunServer();

    void RegisterQuoterService();
    void CreateKesusesAndResources();
    ui64 GetKesusTabletId(const TString& path);
    void CreateKesusResource(const TString& kesusPath, const TString& resourcePath, TMaybe<double> maxUnitsPerSecond = Nothing());
    void CreateKesusResource(ui64 kesusTabletId, const TString& resourcePath, TMaybe<double> maxUnitsPerSecond = Nothing());

    TActorId GetEdgeActor();

    void SetupSettings();

private:
    const TOptions &Opts;
    TPortManager PortManager;
    const ui16 MsgBusPort;
    Tests::TServerSettings::TPtr ServerSettings;

    Tests::TServer::TPtr Server;
    THolder <Tests::TClient> Client;
    TActorId EdgeActor;
};

} // namespace NKikimr
