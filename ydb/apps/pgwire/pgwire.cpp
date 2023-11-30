#include <ydb/core/pgproxy/pg_listener.h>
#include <ydb/core/pgproxy/pg_log.h>
#include <ydb/library/services/services.pb.h>
#include <util/system/mlock.h>
#include <util/stream/file.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include "pgwire.h"
#include "log_impl.h"
#include "pg_ydb_proxy.h"

namespace NPGW {

std::atomic<bool> TPgWire::Quit;

void TPgWire::OnTerminate(int) {
    Quit = true;
}

int TPgWire::Init() {
    ActorSystem->Start();

    ActorSystem->Register(NActors::CreateProcStatCollector(TDuration::Seconds(5), AppData.MetricRegistry));

    Poller = ActorSystem->Register(NActors::CreatePollerActor());

    NYdb::TDriverConfig driverConfig;

    driverConfig.SetEndpoint(Endpoint);

    DatabaseProxy = ActorSystem->Register(CreateDatabaseProxy(driverConfig));
    Listener = ActorSystem->Register(NPG::CreatePGListener(Poller, DatabaseProxy, {
        .Port = ListeningPort,
        .SslCertificatePem = SslCertificate,
    }));

    return 0;
}

int TPgWire::Run() {
    try {
        int res = Init();
        if (res != 0) {
            return res;
        }
#ifndef NDEBUG
        Cout << "Started" << Endl;
#endif
        while (!Quit) {
            Sleep(TDuration::MilliSeconds(100));
        }
#ifndef NDEBUG
        Cout << Endl << "Finished" << Endl;
#endif
        Shutdown();
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
    return 0;
}

int TPgWire::Shutdown() {
    ActorSystem->Stop();
    return 0;
}

TPgWire::TPgWire(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    bool useStdErr = false;
    bool mlock = false;
    TString sslCertificateFile;
    opts.AddLongOption("endpoint", "YDB endpoint to connect").Required().RequiredArgument("ENDPOINT").StoreResult(&Endpoint);
    opts.AddLongOption("port", "Listening port").Optional().DefaultValue("5432").RequiredArgument("PORT").StoreResult(&ListeningPort);
    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&useStdErr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&mlock);
    opts.AddLongOption("ssl-cert-file", "Path to file with SSL certificate including private key").RequiredArgument("PATH").StoreResult(&sslCertificateFile);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (mlock) {
        LockAllMemory(LockCurrentMemory);
    }

    THolder<NActors::TActorSystemSetup> actorSystemSetup = BuildActorSystemSetup();

    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = BuildLoggerSettings();
    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
        loggerSettings,
        useStdErr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("pgwire", false, true),
        NMonitoring::TMetricRegistry::SharedInstance());
    actorSystemSetup->LocalServices.emplace_back(loggerSettings->LoggerActorId, NActors::TActorSetupCmd(loggerActor, NActors::TMailboxType::HTSwap, 0));

    ActorSystem = std::make_unique<NActors::TActorSystem>(actorSystemSetup, &AppData, loggerSettings);
    AppData.MetricRegistry = NMonitoring::TMetricRegistry::SharedInstance();

    if (!sslCertificateFile.empty()) {
        TString sslCertificate = TUnbufferedFileInput(sslCertificateFile).ReadAll();
        if (!sslCertificate.empty()) {
            SslCertificate = sslCertificate;
        } else {
            ythrow yexception() << "Invalid SSL certificate file";
        }
    }
}

TIntrusivePtr<NActors::NLog::TSettings> TPgWire::BuildLoggerSettings() {
    const NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER, NActors::NLog::PRI_WARN);
    loggerSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );
    loggerSettings->Append(
        NKikimrServices::EServiceKikimr_MIN,
        NKikimrServices::EServiceKikimr_MAX,
        NKikimrServices::EServiceKikimr_Name
    );
    TString explanation;
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NKikimrServices::PGWIRE, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NKikimrServices::PGYDB, explanation);
    return loggerSettings;
}

THolder<NActors::TActorSystemSetup> TPgWire::BuildActorSystemSetup() {
    THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[1]);
    setup->ExecutorsCount = 1;
    setup->Executors[0] = new NActors::TBasicExecutorPool(0, 4, 10);

    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));

    return setup;
}

}
