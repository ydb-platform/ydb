#include "channel.h"
#include "channel_p.h"
#include "internals.h"
#include "persqueue_p.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <util/random/random.h>
#include <util/string/builder.h>

#include <grpc++/create_channel.h>

const ui32 MAX_MESSAGE_SIZE = 150 * 1024 * 1024;
const TDuration KEEP_ALIVE_TIME = TDuration::Seconds(90);
const TDuration KEEP_ALIVE_TIMEOUT = TDuration::Seconds(1);
constexpr ui64 MAGIC_COOKIE_VALUE = 123456789;

namespace NPersQueue {

void FillMetaHeaders(grpc::ClientContext& context, const TString& database, ICredentialsProvider* credentials) {
    context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, GetToken(credentials));
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, database);
}

TServerSetting ApplyClusterEndpoint(const TServerSetting& server, const TString& endpoint) {
    TServerSetting ret = server;
    ret.UseLogbrokerCDS = EClusterDiscoveryUsageMode::DontUse;
    TStringBuf endpointWithPort = endpoint;
    TStringBuf host, port;
    ui16 parsedPort = 0;
    if (endpointWithPort.TryRSplit(':', host, port) && TryFromString(port, parsedPort)) {
        ret.Address = TString(host);
        ret.Port = parsedPort;
    } else {
        ret.Address = endpoint;
    }
    return ret;
}

bool UseCDS(const TServerSetting& server) {
    switch (server.UseLogbrokerCDS) {
    case EClusterDiscoveryUsageMode::Auto:
        return server.Address == "logbroker.yandex.net"sv || server.Address == "logbroker-prestable.yandex.net"sv;
    case EClusterDiscoveryUsageMode::Use:
        return true;
    case EClusterDiscoveryUsageMode::DontUse:
        return false;
    }
}

class TChannelImpl::TGetProxyHandler : public IHandler {
public:
    TGetProxyHandler(TChannelImplPtr ptr)
        : Ptr(ptr)
    {}

    void Done() override {
        Ptr->OnGetProxyDone();
    }

    void Destroy(const TError& reason) override {
        Ptr->Destroy(reason);
    }

    TString ToString() override { return "GetProxyHandler"; }

protected:
    TChannelImplPtr Ptr;
};

class TChannelOverCdsBaseImpl::TCdsResponseHandler : public IHandler {
public:
    TCdsResponseHandler(TChannelOverCdsBaseImplPtr ptr)
        : Ptr(ptr)
    {}

    void Done() override {
        Ptr->OnCdsRequestDone();
    }

    void Destroy(const TError& reason) override {
        Ptr->Destroy(reason);
    }

    TString ToString() override { return "CDS-ResponseHandler"; }

protected:
    TChannelOverCdsBaseImplPtr Ptr;
};

TChannel::TChannel(
        const TServerSetting& server, const TCredProviderPtr& credentialsProvider, TPQLibPrivate* pqLib,
        TIntrusivePtr<ILogger> logger, bool preferLocalProxy
) {
    MakeImpl(server, credentialsProvider, pqLib, logger, preferLocalProxy);
}

TChannel::TChannel(
        const TProducerSettings& settings, TPQLibPrivate* pqLib, TIntrusivePtr<ILogger> logger, bool preferLocalProxy
) {
    if (UseCDS(settings.Server))
        Impl = new TProducerChannelOverCdsImpl(settings, pqLib, std::move(logger), preferLocalProxy);
    else
        MakeImpl(settings.Server, settings.CredentialsProvider, pqLib, logger, preferLocalProxy);
}

void TChannel::MakeImpl(
        const TServerSetting& server, const std::shared_ptr<ICredentialsProvider>& credentialsProvider,
        TPQLibPrivate* pqLib, TIntrusivePtr<ILogger> logger, bool preferLocalProxy
) {
    Impl = MakeIntrusive<TChannelOverDiscoveryImpl>(
        server, credentialsProvider, pqLib, std::move(logger), preferLocalProxy
    );
}

NThreading::TFuture<TChannelInfo> TChannel::GetChannel() {
    return Impl->GetChannel();
}

TChannel::~TChannel() {
    Impl->TryCancel();
    Impl->Wait();
}

void TChannel::Start() {
    Impl->Start();
}

TChannelImpl::TChannelImpl(const TServerSetting& server, const std::shared_ptr<ICredentialsProvider>& credentialsProvider, TPQLibPrivate* pqLib, TIntrusivePtr<ILogger> logger, bool preferLocalProxy)
    : Promise(NThreading::NewPromise<TChannelInfo>())
    , Server(server)
    , SelectedEndpoint(Server.GetFullAddressString())
    , CredentialsProvider(credentialsProvider)
    , PreferLocalProxy(preferLocalProxy)
    , Logger(std::move(logger))
    , PQLib(pqLib)
    , CQ(pqLib->GetCompletionQueue())
    , ChooseProxyFinished(0)
{}

TChannelOverDiscoveryImpl::TChannelOverDiscoveryImpl(
        const TServerSetting& server, const TCredProviderPtr& credentialsProvider, TPQLibPrivate* pqLib,
        TIntrusivePtr<ILogger> logger, bool preferLocalProxy
)
    : TChannelImpl(server, credentialsProvider, pqLib, logger, preferLocalProxy)
{
    CreationTimeout = PQLib->GetSettings().ChannelCreationTimeout;
}

TChannelOverCdsBaseImpl::TChannelOverCdsBaseImpl(const TServerSetting& server, const TCredProviderPtr& credentialsProvider, TPQLibPrivate* pqLib,
                                                 TIntrusivePtr<ILogger> logger, bool preferLocalProxy
)
    : TChannelOverDiscoveryImpl(server, credentialsProvider, pqLib, logger, preferLocalProxy)
    , CdsRequestFinished(0)
{}

void TChannelOverDiscoveryImpl::Start() {
    Channel = CreateGrpcChannel(SelectedEndpoint);
    Stub = Ydb::Discovery::V1::DiscoveryService::NewStub(Channel);
    IHandlerPtr handler = new TGetProxyHandler(this);
    Context.set_deadline(TInstant::Now() + CreationTimeout);
    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(Server.Database);
    FillMetaHeaders(Context, Server.Database, CredentialsProvider.get());
    DEBUG_LOG("Send list endpoints request: " << request, "", "");
    Rpc = Stub->AsyncListEndpoints(&Context, request, CQ.get());
    Rpc->Finish(&Response, &Status, new TQueueEvent(std::move(handler)));
}

void TChannelOverCdsBaseImpl::Start() {
    Channel = CreateGrpcChannel(Server.GetFullAddressString());
    Stub = Ydb::PersQueue::V1::ClusterDiscoveryService::NewStub(Channel);
    IHandlerPtr handler = new TCdsResponseHandler(this);
    CdsContext.set_deadline(TInstant::Now() + PQLib->GetSettings().ChannelCreationTimeout);
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest request = GetCdsRequest();
    FillMetaHeaders(CdsContext, Server.Database, CredentialsProvider.get());
    DEBUG_LOG("Send cds request: " << request, "", "");
    CreationStartTime = TInstant::Now();
    Rpc = Stub->AsyncDiscoverClusters(&CdsContext, request, CQ.get());
    Rpc->Finish(&Response, &Status, new TQueueEvent(std::move(handler)));
}

NThreading::TFuture<TChannelInfo> TChannelImpl::GetChannel() {
    return Promise.GetFuture();
}

bool TChannelImpl::TryCancel() {
    if (AtomicSwap(&ChooseProxyFinished, 1) == 0) {
        Y_ASSERT(!Promise.HasValue());
        Promise.SetValue(TChannelInfo{nullptr, 0});
        Context.TryCancel();
        return true;
    }
    return false;
}

TChannelImpl::~TChannelImpl() = default;

std::shared_ptr<grpc::Channel> TChannelImpl::CreateGrpcChannel(const TString& address) {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, KEEP_ALIVE_TIME.MilliSeconds());
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, KEEP_ALIVE_TIMEOUT.MilliSeconds());
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    args.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, KEEP_ALIVE_TIME.MilliSeconds());

    args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, MAX_MESSAGE_SIZE);
    args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, MAX_MESSAGE_SIZE);

    //TODO: fill there other settings from PQLib
    DEBUG_LOG("Creating grpc channel to \"" << address << "\"", "", "");
    if (Server.UseSecureConnection)
        return grpc::CreateCustomChannel(address,
                                         grpc::SslCredentials(grpc::SslCredentialsOptions{Server.CaCert, "", ""}),
                                         args);
    else
        return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
}

TString TChannelImpl::GetProxyAddress(const TString& proxyName, ui32 port) {
    TStringBuilder address;
    if (proxyName.find(':') != TString::npos && proxyName.find('[') == TString::npos) { //fix for ipv6 adresses
        address << "[" << proxyName << "]";
    } else {
        address << proxyName;
    }
    if (port)
        address << ":" << port;
    else
        address << ":" << Server.Port;
    return std::move(address);
}

void TChannelImpl::OnGetProxyDone() {
    if (AtomicSwap(&ChooseProxyFinished, 1) == 0) {
        const bool ok = Status.ok();
        Y_ASSERT(!Promise.HasValue());
        if (ok) {
            ProcessGetProxyResponse();
        } else {
            if (Status.error_code() == grpc::CANCELLED) {
                INFO_LOG("Grpc request is canceled by user", "", "");
            } else {
                const auto& msg = Status.error_message();
                TString res(msg.data(), msg.length());
                ERR_LOG("Grpc error " << static_cast<int>(Status.error_code()) << ": \"" << res << "\"", "", "");
            }
            StartFailed();
        }
    }
    SetDone();
}

void TChannelImpl::StartFailed() {
    AtomicSet(ChooseProxyFinished, 1);
    Promise.SetValue(TChannelInfo{nullptr, 0});
}

void TChannelOverDiscoveryImpl::ProcessGetProxyResponse() {
    Ydb::Discovery::ListEndpointsResult result;
    auto& reply = Response.operation().result();
    reply.UnpackTo(&result);

    auto has_service = [](const Ydb::Discovery::EndpointInfo& endpoint) {
        for (auto& service: endpoint.service()) {
            if (service == "pq")
                return true;
        }
        return false;
    };
    TVector<std::pair<TString, ui32>> validEps;
    for (const auto& ep : result.endpoints()) {
        if (has_service(ep) && (!Server.UseSecureConnection || ep.ssl())) {
            validEps.emplace_back(ep.address(), ep.port());
        }
    }
    if (validEps.empty()) {
        ERR_LOG("No valid endpoints to connect!", "", "");
        StartFailed();
    } else {
        auto& selected = validEps[RandomNumber<ui32>(validEps.size())];
        DEBUG_LOG("Selected endpoint {\"" << selected.first << "\", " << selected.second << "}", "", "");
        auto channel = CreateGrpcChannel(GetProxyAddress(selected.first, selected.second));
        Promise.SetValue(TChannelInfo{std::move(channel), MAGIC_COOKIE_VALUE});
    }
}

void TChannelOverCdsBaseImpl::OnCdsRequestDone() {
    DEBUG_LOG("ON CDS request done: " << Response, "", "");

    if (AtomicSwap(&CdsRequestFinished, 1) == 0) {
        const bool ok = Status.ok();
        if (ok) {
            if (ProcessCdsResponse()) {
                CreationTimeout = CreationTimeout - (TInstant::Now() - CreationStartTime);
                TChannelOverDiscoveryImpl::Start();
                // Don't set AllDone event, because we are about to continue node discovery
                // via TChannelOverDiscoveryImpl part of implementation.
                return;
            } else {
                StartFailed();
            }
        } else {
            if (Status.error_code() == grpc::CANCELLED) {
                INFO_LOG("Grpc request is canceled by user", "", "");
            } else {
                const auto& msg = Status.error_message();
                TString res(msg.data(), msg.length());
                ERR_LOG("Grpc error " << static_cast<int>(Status.error_code()) << ": \"" << res << "\"", "", "");
            }
            StartFailed();
        }
    }
    SetDone();
}

bool TChannelOverCdsBaseImpl::TryCancel() {
    if (AtomicSwap(&CdsRequestFinished, 1) == 0) {
        Y_ASSERT(!Promise.HasValue());
        Promise.SetValue(TChannelInfo{nullptr, 0});
        CdsContext.TryCancel();
        return true;
    } else {
        TChannelOverDiscoveryImpl::TryCancel();
    }
    return false;
}

TProducerChannelOverCdsImpl::TProducerChannelOverCdsImpl(const TProducerSettings& settings, TPQLibPrivate* pqLib,
                                                         TIntrusivePtr<ILogger> logger, bool preferLocalProxy)
    : TChannelOverCdsBaseImpl(settings.Server, settings.CredentialsProvider, pqLib, logger, preferLocalProxy)
    , Topic(settings.Topic)
    , SourceId(settings.SourceId)
    , PreferredCluster(settings.PreferredCluster)
{
}

Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest TProducerChannelOverCdsImpl::GetCdsRequest() const {
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest request;
    auto* params = request.add_write_sessions();
    params->set_topic(Topic);
    params->set_source_id(SourceId);
    params->set_preferred_cluster_name(PreferredCluster); // Can be empty.
    return request;
}

bool TProducerChannelOverCdsImpl::ProcessCdsResponse() {
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult result;
    auto& reply = Response.operation().result();
    reply.UnpackTo(&result);
    DEBUG_LOG("Process CDS result: " << result, "", "");
    for (auto& session: result.write_sessions_clusters()) {
        for (auto& cluster: session.clusters()) {
            if (cluster.available()) {
                const TServerSetting selectedServer = ApplyClusterEndpoint(Server, cluster.endpoint());
                SelectedEndpoint = GetProxyAddress(selectedServer.Address, selectedServer.Port);
                return true;
            }
        }
    }
    ERR_LOG("Could not find valid cluster in CDS response", "", "");
    return false;
}

TConsumerChannelOverCdsImpl::TConsumerChannelOverCdsImpl(const TConsumerSettings& settings, TPQLibPrivate* pqLib,
                                                         TIntrusivePtr<ILogger> logger)
    : TChannelOverCdsBaseImpl(settings.Server, settings.CredentialsProvider, pqLib, logger, true)
    , Result(std::make_shared<TResult>())
    , Topics(settings.Topics)
{
}

Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest TConsumerChannelOverCdsImpl::GetCdsRequest() const {
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest request;
    for (const TString& topic : Topics) {
        auto* params = request.add_read_sessions();
        params->set_topic(topic);
        params->mutable_all_original(); // set all_original
    }
    return request;
}

bool TConsumerChannelOverCdsImpl::ProcessCdsResponse() {
    auto& reply = Response.operation().result();
    reply.UnpackTo(&Result->second);
    Result->first = Status;
    DEBUG_LOG("Got CDS result: " << Result->second, "", "");
    return false; // Signals future
}

void TConsumerChannelOverCdsImpl::StartFailed() {
    Result->first = Status;
    TChannelOverCdsBaseImpl::StartFailed();
}

void TChannelImpl::SetDone() {
    AllDoneEvent.Signal();
}

void TChannelImpl::Destroy(const TError& error) {
    if (AtomicSwap(&ChooseProxyFinished, 1) == 0) {
        const auto& msg = Status.error_message();
        TString res(msg.data(), msg.length());

        ERR_LOG("Got proxy error response " << error << " grpc error " << res, "", "");
        //TODO: add here status from Status to response, log status

        Y_ASSERT(!Promise.HasValue());
        Promise.SetValue(TChannelInfo{nullptr, 0});
    }

    SetDone();
}

void TChannelImpl::Wait() {
    AllDoneEvent.Wait();
}

}
