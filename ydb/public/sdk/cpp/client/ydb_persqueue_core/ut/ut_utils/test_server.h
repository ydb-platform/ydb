#pragma once
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

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
    {
        if (start) {
            StartServer();
        }
    }
    TTestServer(const NKikimr::Tests::TServerSettings& settings, bool start = true)
        : PortManager(MakeSimpleShared<TPortManager>())
        , Port(PortManager->GetPort(2134))
        , GrpcPort(PortManager->GetPort(2135))
        , ServerSettings(settings)
        , GrpcServerOptions(NGrpc::TServerOptions().SetHost("[::1]").SetPort(GrpcPort))
    {
        ServerSettings.Port = Port;
        ServerSettings.SetGrpcPort(GrpcPort);
        if (start)
            StartServer();
    }

    void StartServer(bool doClientInit = true, TMaybe<TString> databaseName = Nothing()) {
        PrepareNetDataFile();
        CleverServer = MakeHolder<NKikimr::Tests::TServer>(ServerSettings);
        CleverServer->EnableGRpc(GrpcServerOptions);
        EnableLogs();

        Cerr << "TTestServer started on Port " << Port << " GrpcPort " << GrpcPort << Endl;

        AnnoyingClient = MakeHolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(ServerSettings, GrpcPort, databaseName);
        if (doClientInit) {
            AnnoyingClient->FullInit();
            AnnoyingClient->CheckClustersList(CleverServer->GetRuntime());
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

    void EnableLogs(const TVector<NKikimrServices::EServiceKikimr>& services = LOGGED_SERVICES,
                    NActors::NLog::EPriority prio = NActors::NLog::PRI_DEBUG) {
        Y_VERIFY(CleverServer != nullptr, "Start server before enabling logs");
        for (auto s : services) {
            CleverServer->GetRuntime()->SetLogPriority(s, prio);
        }
    }

    void WaitInit(const TString& topic) {
        AnnoyingClient->WaitTopicInit(topic);
    }

    bool PrepareNetDataFile(const TString& content = "::1/128\tdc1") {
        if (NetDataFile)
            return false;
        NetDataFile = MakeHolder<TTempFileHandle>();
        NetDataFile->Write(content.Data(), content.Size());
        NetDataFile->FlushData();
        ServerSettings.NetClassifierConfig.SetNetDataFilePath(NetDataFile->Name());
        return true;
    }

    void UpdateDC(const TString& name, bool local, bool enabled) {
        AnnoyingClient->UpdateDC(name, local, enabled);
    }

    const NYdb::TDriver& GetDriver() const {
        return CleverServer->GetDriver();
    }

public:
    TSimpleSharedPtr<TPortManager> PortManager;
    ui16 Port;
    ui16 GrpcPort;

    THolder<NKikimr::Tests::TServer> CleverServer;
    NKikimr::Tests::TServerSettings ServerSettings;
    NGrpc::TServerOptions GrpcServerOptions;
    THolder<TTempFileHandle> NetDataFile;

    THolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> AnnoyingClient;


    static const TVector<NKikimrServices::EServiceKikimr> LOGGED_SERVICES;
};

} // namespace NPersQueue
