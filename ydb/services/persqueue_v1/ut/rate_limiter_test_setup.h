#pragma once
#include "pq_data_writer.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/test_pq_client.h>

namespace NKikimr::NPersQueueTests {

class TRateLimiterTestSetup {
public:
    explicit TRateLimiterTestSetup(
        NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity limitedEntity,
        double writeAccountQuota = 1000.0,
        double readAccountQuota = 1000.0,
        bool enableReadQuoting = false
    );

    void CreateTopic(const TString& path);
    void CreateConsumer(const TString& path);

    // namespace NPersQueue = Ydb::PersQueue;
//    THolder<Ydb::PersQueue::IProducer> StartProducer(const TString& topicPath, bool compress = false);

    // Getters
    ui16 GetGrpcPort() const {
        return Server->GrpcPort;
    }

    ui16 GetMsgBusPort() const {
        return Server->Port;
    }

    NKikimr::Tests::TServer& GetServer() {
        return *Server->CleverServer;
    }

    NPersQueueTests::TFlatMsgBusPQClient& GetClient() {
        return *Server->AnnoyingClient;
    }

private:
    void CreateKesus(const TString& account);
    void CreateQuotaResources(const TString& path, const TString& quotaPrefix, bool excludeLastComponent);

    void Start(bool enableReadQuoting);

    void InitServer(bool enableReadQuoting);
    void InitQuoting();
    void WaitWritePQServiceInitialization();

private:
    THolder<NPersQueue::TTestServer> Server;
    THolder<TPQDataWriter> PQDataWriter; // For waiting for grpc writer service initialization.
    const NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity LimitedEntity;
    double WriteAccountQuota;
    double ReadAccountQuota;
    const TString QuotersRootPath = "/Root/PersQueue/System/Quoters";
};
}
