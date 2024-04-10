#include "rate_limiter_test_setup.h"
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <library/cpp/logger/priority.h>

using namespace NPersQueue;
using namespace Ydb::PersQueue;
using namespace NKikimr::Tests;


namespace NKikimr::NPersQueueTests {

TRateLimiterTestSetup::TRateLimiterTestSetup(
    NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity limitedEntity,
    double writeAccountQuota,
    double readAccountQuota,
    bool enableReadQuoting
)
    : Server(new NPersQueue::TTestServer(false))
    , LimitedEntity(limitedEntity)
    , WriteAccountQuota(writeAccountQuota)
    , ReadAccountQuota(readAccountQuota)
{
    Start(enableReadQuoting);
}

void TRateLimiterTestSetup::CreateTopic(const TString& path) {
    const TString name = BuildFullTopicName(path, "dc1");
    const TString account = GetAccount(name);

    Cerr << "Creating topic \"" << name << "\"" << Endl;
    Server->AnnoyingClient->CreateTopic(name, 1);

    CreateKesus(account);
    CreateQuotaResources(path, "write-quota", false);
    CreateQuotaResources(path, "read-quota", true);
}

void TRateLimiterTestSetup::CreateConsumer(const TString& path) {
    const TString account = GetAccount(path);

    Cerr << "Creating consumer \"" << path << "\"" << Endl;
    Server->AnnoyingClient->CreateConsumer(path);

    CreateKesus(account);
    CreateQuotaResources(path, "write-quota", true);
    CreateQuotaResources(path, "read-quota", false);
}

void TRateLimiterTestSetup::CreateKesus(const TString& account) {
    const NMsgBusProxy::EResponseStatus createKesusResult = Server->AnnoyingClient->CreateKesus(QuotersRootPath, account);
    UNIT_ASSERT_C(createKesusResult == NMsgBusProxy::MSTATUS_OK, createKesusResult);

    const TString kesusPath = TStringBuilder() << QuotersRootPath << "/" << account;

    Cerr << "Creating kesus with path=" << kesusPath << Endl;
    auto setAccountQuota = [&](const TString& quotaPrefix, double value) {
        Cerr << "Adding quota for account kesus=" << kesusPath << " quota-path=" << quotaPrefix << " value=" << value << Endl;
        const auto statusCode = Server->AnnoyingClient->AddQuoterResource(
                Server->CleverServer->GetRuntime(), kesusPath, quotaPrefix, value
        );
        UNIT_ASSERT_C(
            statusCode == Ydb::StatusIds::SUCCESS || statusCode == Ydb::StatusIds::ALREADY_EXISTS,
            "Status: " << Ydb::StatusIds::StatusCode_Name(statusCode)
        );
    };
    setAccountQuota("write-quota", WriteAccountQuota);
    setAccountQuota("read-quota", ReadAccountQuota);
}

void TRateLimiterTestSetup::CreateQuotaResources(const TString& path, const TString& quotaPrefix, bool excludeLastComponent) {
    TVector<TString> pathComponents = SplitPath(path);
    if (pathComponents.size() <= 1) {
        return;
    }
    TStringBuilder prefixPath;
    prefixPath << quotaPrefix;

    auto firstIt = pathComponents.begin() + 1; // resource path must be without account
    auto lastIt = pathComponents.end() - (excludeLastComponent ? 1 : 0);

    const TString account = GetAccount(path);
    const TString kesusPath = TStringBuilder() << QuotersRootPath << "/" << account;
    for (auto currentComponent = firstIt; currentComponent != lastIt; ++currentComponent) {
        prefixPath << "/" << *currentComponent;
        Cerr << "Adding quoter resource: \"" << prefixPath << "\"" << Endl;
        const auto statusCode = Server->AnnoyingClient->AddQuoterResource(
                Server->CleverServer->GetRuntime(), kesusPath, prefixPath
        );
        UNIT_ASSERT_C(
            statusCode == Ydb::StatusIds::SUCCESS || statusCode == Ydb::StatusIds::ALREADY_EXISTS,
            "Status: " << Ydb::StatusIds::StatusCode_Name(statusCode)
        );
    }
}

/*
THolder<Ydb::PersQueue::IProducer> TRateLimiterTestSetup::StartProducer(const TString& topicPath, bool compress) {
    Ydb::PersQueue::TProducerSettings producerSettings;
    producerSettings.Server = Ydb::PersQueue::TServerSetting("localhost", Server->GrpcPort);
    producerSettings.Topic = topicPath;
    producerSettings.SourceId = "TRateLimiterTestSetupSourceId";
    producerSettings.Codec = compress ? "gzip" : "raw";
    THolder<Ydb::PersQueue::IProducer> producer = PQLib->CreateProducer(producerSettings);
    auto startResult = producer->Start();
    UNIT_ASSERT_EQUAL_C(Ydb::StatusIds::SUCCESS, startResult.GetValueSync().Response.status(), "Response: " << startResult.GetValueSync().Response);
    return producer;
}
*/

void TRateLimiterTestSetup::Start(bool enableReadQuoting) {
    InitServer(enableReadQuoting);
    InitQuoting();
    WaitWritePQServiceInitialization();
}

void TRateLimiterTestSetup::InitServer(bool enableReadQuoting) {
    auto& settings = Server->ServerSettings;

    settings.PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);
    settings.PQConfig.MutableQuotingConfig()->SetEnableReadQuoting(enableReadQuoting);
    settings.PQConfig.MutableQuotingConfig()->SetTopicWriteQuotaEntityToLimit(LimitedEntity);

    Server->GrpcServerOptions.SetMaxMessageSize(130_MB);
    Server->StartServer();
    Server->EnableLogs(
        {
            NKikimrServices::PQ_READ_PROXY,
            NKikimrServices::PQ_WRITE_PROXY,
            NKikimrServices::PERSQUEUE,
            NKikimrServices::QUOTER_SERVICE,
            NKikimrServices::QUOTER_PROXY,
            NKikimrServices::KESUS_TABLET,
            NKikimrServices::PQ_RATE_LIMITER
        },
        NActors::NLog::PRI_TRACE
    );
}

void TRateLimiterTestSetup::InitQuoting() {
    Server->AnnoyingClient->MkDir("/Root", "PersQueue");
    Server->AnnoyingClient->MkDir("/Root/PersQueue", "System");
    Server->AnnoyingClient->MkDir("/Root/PersQueue/System", "Quoters");
}

void TRateLimiterTestSetup::WaitWritePQServiceInitialization() {
    PQDataWriter = MakeHolder<TPQDataWriter>("writer_source_id", *Server);
}

}
