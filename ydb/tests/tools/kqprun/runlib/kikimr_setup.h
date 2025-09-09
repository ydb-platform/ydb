#pragma once

#include "settings.h"

#include <library/cpp/logger/backend.h>

#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/testlib/test_client.h>

namespace NKikimrRun {

class TKikimrSetupBase {
public:
    TKikimrSetupBase() = default;

    virtual ~TKikimrSetupBase() = default;

    TAutoPtr<TLogBackend> CreateLogBackend(const TServerSettings& settings) const;

    NKikimr::Tests::TServerSettings GetServerSettings(const TServerSettings& settings, ui32 grpcPort, bool verbosity);

    NYdbGrpc::TServerOptions GetGrpcSettings(ui32 grpcPort, ui32 nodeIdx, TDuration shutdownDeadline = TDuration::Seconds(30)) const;

    static std::optional<NKikimrWhiteboard::TSystemStateInfo> GetSystemStateInfo(TIntrusivePtr<NKikimr::NMemory::IProcessMemoryInfoProvider> memoryInfoProvider);

    static TString FormatMonitoringLink(ui16 port, const TString& uri);

    static TString FormatGrpcLink(ui16 port);

    virtual NActors::TTestActorRuntime* GetRuntime() const = 0;

private:
    void SetLoggerSettings(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const;

    void SetFunctionRegistry(const TServerSettings& settings, NKikimr::Tests::TServerSettings& serverSettings) const;

protected:
    TPortManager PortManager;
};

}  // namespace NKikimrRun
