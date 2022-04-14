#pragma once
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <ydb/core/testlib/test_pq_client.h>
#include <library/cpp/grpc/server/grpc_server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/tempfile.h>

namespace NPersQueue {

static constexpr int DEBUG_LOG_LEVEL = 7;

class TTestServer {
public:
    TTestServer(bool start = true, TMaybe<TSimpleSharedPtr<TPortManager>> portManager = Nothing())
        : PortManager(portManager.GetOrElse(MakeSimpleShared<TPortManager>()))
        , Port(PortManager->GetPort(2134))
        , GrpcPort(PortManager->GetPort(2135))
        , ServerSettings(NKikimr::NPersQueueTests::PQSettings(Port).SetGrpcPort(GrpcPort))
        , GrpcServerOptions(NGrpc::TServerOptions().SetHost("[::1]").SetPort(GrpcPort))
        , PQLibSettings(TPQLibSettings{ .DefaultLogger = new TCerrLogger(DEBUG_LOG_LEVEL) })
        , PQLib(new TPQLib(PQLibSettings))
    {
        PatchServerSettings();
        StartIfNeeded(start);
    }

    TTestServer(const NKikimr::Tests::TServerSettings& settings, bool start = true)
        : PortManager(MakeSimpleShared<TPortManager>())
        , Port(PortManager->GetPort(2134))
        , GrpcPort(PortManager->GetPort(2135))
        , ServerSettings(settings)
        , GrpcServerOptions(NGrpc::TServerOptions().SetHost("[::1]").SetPort(GrpcPort))
        , PQLibSettings(TPQLibSettings{ .DefaultLogger = new TCerrLogger(DEBUG_LOG_LEVEL) })
        , PQLib(new TPQLib(PQLibSettings))
    {
        ServerSettings.Port = Port;
        ServerSettings.SetGrpcPort(GrpcPort);
        PatchServerSettings();
        StartIfNeeded(start);
    }

    void StartServer(bool doClientInit = true) {
        PrepareNetDataFile();
        CleverServer = MakeHolder<NKikimr::Tests::TServer>(ServerSettings);
        CleverServer->EnableGRpc(GrpcServerOptions);
        AnnoyingClient = MakeHolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(ServerSettings, GrpcPort);
        EnableLogs(LOGGED_SERVICES);
        if (doClientInit) {
            AnnoyingClient->FullInit();
        }
    }

    void ShutdownGRpc() {
        CleverServer->ShutdownGRpc();
    }

    void EnableGRpc() {
        CleverServer->EnableGRpc(GrpcServerOptions);
    }

    void ShutdownServer() {
        CleverServer = nullptr;
    }

    void RestartServer() {
        ShutdownServer();
        StartServer();
    }

    void EnableLogs(const TVector<NKikimrServices::EServiceKikimr> services,
                    NActors::NLog::EPriority prio = NActors::NLog::PRI_DEBUG) {
        Y_VERIFY(CleverServer != nullptr, "Start server before enabling logs");
        for (auto s : services) {
            CleverServer->GetRuntime()->SetLogPriority(s, prio);
        }
    }

    void WaitInit(const TString& topic) {
        TProducerSettings s;
        s.Topic = topic;
        s.Server = TServerSetting{"localhost", GrpcPort};
        s.SourceId = "src";

        while (PQLib->CreateProducer(s, {}, false)->Start().GetValueSync().Response.HasError()) {
            Sleep(TDuration::MilliSeconds(200));
        }
    }

    bool PrepareNetDataFile(const TString& content = "::1/128\tdc1") {
        if (NetDataFile)
            return false;
        NetDataFile = MakeHolder<TTempFileHandle>("netData.tsv");
        NetDataFile->Write(content.Data(), content.Size());
        NetDataFile->FlushData();
        ServerSettings.NetClassifierConfig.SetNetDataFilePath(NetDataFile->Name());
        return true;
    }

    void UpdateDC(const TString& name, bool local, bool enabled) {
        AnnoyingClient->UpdateDC(name, local, enabled);
    }

private:
    void PatchServerSettings();
    void StartIfNeeded(bool start);

public:
    TSimpleSharedPtr<TPortManager> PortManager;
    ui16 Port;
    ui16 GrpcPort;

    THolder<NKikimr::Tests::TServer> CleverServer;
    NKikimr::Tests::TServerSettings ServerSettings;
    NGrpc::TServerOptions GrpcServerOptions;
    THolder<TTempFileHandle> NetDataFile;

    THolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> AnnoyingClient;

    TPQLibSettings PQLibSettings;
    THolder<TPQLib> PQLib;

    static const TVector<NKikimrServices::EServiceKikimr> LOGGED_SERVICES;
};

} // namespace NPersQueue
