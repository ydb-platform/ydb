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
    TTestServer(bool start = true, TMaybe<TSimpleSharedPtr<TPortManager>> portManager = Nothing(),
                const TVector<NKikimrServices::EServiceKikimr>& logServices = TTestServer::LOGGED_SERVICES, NActors::NLog::EPriority logPriority = NActors::NLog::PRI_DEBUG)
        : PortManager(portManager.GetOrElse(MakeSimpleShared<TPortManager>()))
        , Port(PortManager->GetPort(2134))
        , GrpcPort(PortManager->GetPort(2135))
        , ServerSettings(NKikimr::NPersQueueTests::PQSettings(Port).SetGrpcPort(GrpcPort))
        , GrpcServerOptions(NGrpc::TServerOptions().SetHost("[::1]").SetPort(GrpcPort))
    {
        auto loggerInitializer = [logServices, logPriority](NActors::TTestActorRuntime& runtime) {
            for (auto s : logServices)
                runtime.SetLogPriority(s, logPriority);
        };
        ServerSettings.SetLoggerInitializer(loggerInitializer);

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
        Log.SetFormatter([](ELogPriority priority, TStringBuf message) {
            return TStringBuilder() << TInstant::Now() << " " << priority << ": " << message << Endl;
        });

        PrepareNetDataFile();

        CleverServer = MakeHolder<NKikimr::Tests::TServer>(ServerSettings);
        CleverServer->EnableGRpc(GrpcServerOptions);

        Log << TLOG_INFO << "TTestServer started on Port " << Port << " GrpcPort " << GrpcPort;

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

    void KillTopicPqrbTablet(const TString& topicPath) {
        KillTopicTablets(topicPath, true, false);
    }

    void KillTopicPqTablets(const TString& topicPath) {
        KillTopicTablets(topicPath, false, true);
    }

private:
    void KillTopicTablets(const TString& topicPath, bool killPqrb, bool killPq) {
        auto describeResult = AnnoyingClient->Ls(topicPath);
        UNIT_ASSERT_C(describeResult->Record.GetPathDescription().HasPersQueueGroup(), describeResult->Record);
        auto persQueueGroup = describeResult->Record.GetPathDescription().GetPersQueueGroup();

        if (killPqrb)
        {
            Log << TLOG_INFO << "Kill PQRB tablet " << persQueueGroup.GetBalancerTabletID();
            AnnoyingClient->KillTablet(*CleverServer, persQueueGroup.GetBalancerTabletID());
        }

        if (killPq)
        {
            THashSet<ui64> restartedTablets;
            for (const auto& p : persQueueGroup.GetPartitions())
                if (restartedTablets.insert(p.GetTabletId()).second)
                {
                    Log << TLOG_INFO << "Kill PQ tablet " << p.GetTabletId();
                    AnnoyingClient->KillTablet(*CleverServer, p.GetTabletId());
                }
        }

        CleverServer->GetRuntime()->DispatchEvents();
    }

public:
    TString TestCaseName;

    TSimpleSharedPtr<TPortManager> PortManager;
    ui16 Port;
    ui16 GrpcPort;

    THolder<NKikimr::Tests::TServer> CleverServer;
    NKikimr::Tests::TServerSettings ServerSettings;
    NGrpc::TServerOptions GrpcServerOptions;
    THolder<TTempFileHandle> NetDataFile;

    TLog Log = CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG);

    THolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> AnnoyingClient;


    static const TVector<NKikimrServices::EServiceKikimr> LOGGED_SERVICES;
};

} // namespace NPersQueue
