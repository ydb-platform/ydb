#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include "channel.h"
#include "scheduler.h"
#include "internals.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>
#include <library/cpp/threading/future/future.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <deque>

namespace NPersQueue {

class TPQLibPrivate;

// Apply endpoint that was taken from CDS.
TServerSetting ApplyClusterEndpoint(const TServerSetting& server, const TString& endpoint);

class TChannelImpl : public TAtomicRefCount<TChannelImpl> {
public:
    NThreading::TFuture<TChannelInfo> GetChannel();

    virtual ~TChannelImpl();

    virtual bool TryCancel();

    TChannelImpl(const TServerSetting& server, const TCredProviderPtr& credentialsProvider, TPQLibPrivate* pqLib,
                 TIntrusivePtr<ILogger> logger, bool preferLocalProxy);

    virtual void Start() = 0;

    void Wait();

protected:
    friend class TPQLibPrivate;
    class TGetProxyHandler;

    void Destroy(const TError& error);
    virtual void StartFailed();
    void SetDone();
    void OnGetProxyDone();
    virtual void ProcessGetProxyResponse() = 0;
    std::shared_ptr<grpc::Channel> CreateGrpcChannel(const TString& address);
    TString GetProxyAddress(const TString& proxyName, ui32 port = 0);
    TString GetToken();

    NThreading::TPromise<TChannelInfo> Promise;
    TServerSetting Server;
    TString SelectedEndpoint;
    std::shared_ptr<ICredentialsProvider> CredentialsProvider;
    bool PreferLocalProxy;

    TIntrusivePtr<ILogger> Logger;
    TPQLibPrivate* PQLib;

protected:
    std::shared_ptr<grpc::CompletionQueue> CQ;
    std::shared_ptr<grpc::Channel> Channel;
    grpc::ClientContext Context;
    grpc::Status Status;

    TAutoEvent AllDoneEvent;

    TAtomic ChooseProxyFinished;
};

class TChannelOverDiscoveryImpl : public TChannelImpl {
public:
    TChannelOverDiscoveryImpl(const TServerSetting& server, const TCredProviderPtr&, TPQLibPrivate* pqLib,
                              TIntrusivePtr<ILogger> logger, bool preferLocalProxy);
    void Start() override;

protected:
    void ProcessGetProxyResponse() override;
    TDuration CreationTimeout;

private:
    std::unique_ptr<Ydb::Discovery::V1::DiscoveryService::Stub> Stub;
    std::unique_ptr<grpc::ClientAsyncResponseReader<Ydb::Discovery::ListEndpointsResponse>> Rpc;
    Ydb::Discovery::ListEndpointsResponse Response;
};


class TChannelOverCdsBaseImpl : public TChannelOverDiscoveryImpl {
public:
    TChannelOverCdsBaseImpl(const TServerSetting& server, const TCredProviderPtr&, TPQLibPrivate* pqLib,
                            TIntrusivePtr<ILogger> logger, bool preferLocalProxy);
    void Start() override;
    void OnCdsRequestDone();

    bool TryCancel() override;

private:
    virtual Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest GetCdsRequest() const = 0;
    virtual bool ProcessCdsResponse() = 0;

protected:
    class TCdsResponseHandler;

private:
    std::unique_ptr<Ydb::PersQueue::V1::ClusterDiscoveryService::Stub> Stub;
    std::unique_ptr<grpc::ClientAsyncResponseReader<Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse>> Rpc;

protected:
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse Response;

private:
    grpc::ClientContext CdsContext;

    TInstant CreationStartTime = TInstant::Zero();
    TAtomic CdsRequestFinished;
};
using TChannelOverCdsBaseImplPtr = TIntrusivePtr<TChannelOverCdsBaseImpl>;

class TProducerChannelOverCdsImpl : public TChannelOverCdsBaseImpl {
public:
    TProducerChannelOverCdsImpl(const TProducerSettings& settings, TPQLibPrivate* pqLib,
                                TIntrusivePtr<ILogger> logger, bool preferLocalProxy);

private:
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest GetCdsRequest() const override;
    bool ProcessCdsResponse() override;

private:
    TString Topic;
    TString SourceId;
    TString PreferredCluster;
};

class TConsumerChannelOverCdsImpl : public TChannelOverCdsBaseImpl {
public:
    using TResult = std::pair<grpc::Status, Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult>;
    using TResultPtr = std::shared_ptr<TResult>;

public:
    TConsumerChannelOverCdsImpl(const TConsumerSettings& settings, TPQLibPrivate* pqLib,
                                TIntrusivePtr<ILogger> logger);

    TResultPtr GetResultPtr() const {
        return Result;
    }

private:
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest GetCdsRequest() const override;
    bool ProcessCdsResponse() override;
    void StartFailed() override;

private:
    TResultPtr Result;
    TVector<TString> Topics;
};

}
