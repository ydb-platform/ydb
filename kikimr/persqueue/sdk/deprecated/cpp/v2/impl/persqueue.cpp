#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include "internals.h"
#include "persqueue_p.h"
#include "compat_producer.h"
#include "consumer.h"
#include "multicluster_consumer.h"
#include "producer.h"
#include "retrying_consumer.h"
#include "retrying_producer.h"
#include "multicluster_producer.h"
#include "processor.h"
#include "compressing_producer.h"
#include "decompressing_consumer.h"
#include "local_caller.h"
#include "ydb_sdk_consumer.h"

#include <util/digest/numeric.h>
#include <util/system/thread.h>

namespace NPersQueue {


TString TPQLib::GetUserAgent() const {
    return Impl->GetUserAgent();
}

void TPQLib::SetUserAgent(const TString& userAgent) {
    Impl->SetUserAgent(userAgent);
}

TPQLib::TPQLib(const TPQLibSettings& settings)
    : Impl(new TPQLibPrivate(settings))
{}

TPQLib::~TPQLib() {
    AtomicSet(Alive, 0);
    Impl->CancelObjectsAndWait();
}

#define CHECK_PQLIB_ALIVE() Y_VERIFY(AtomicGet(Alive), "Attempt to use PQLib after/during its destruction.")

THolder<IProducer> TPQLib::CreateMultiClusterProducer(const TMultiClusterProducerSettings& settings, TIntrusivePtr<ILogger> logger, bool deprecated) {
    CHECK_PQLIB_ALIVE();
    return MakeHolder<TPublicProducer>(Impl->CreateMultiClusterProducer(settings, deprecated, std::move(logger)));
}

THolder<IProducer> TPQLib::CreateProducer(const TProducerSettings& settings, TIntrusivePtr<ILogger> logger, bool deprecated) {
    CHECK_PQLIB_ALIVE();
    auto producer = settings.ReconnectOnFailure ?
        Impl->CreateRetryingProducer(settings, deprecated, std::move(logger)) :
        Impl->CreateProducer(settings, deprecated, std::move(logger));
    return MakeHolder<TPublicProducer>(std::move(producer));
}

template <class TConsumerOrProducerSettings>
NYdb::NPersQueue::TPersQueueClient& TPQLibPrivate::GetOrCreatePersQueueClient(const TConsumerOrProducerSettings& settings, const TIntrusivePtr<ILogger>& logger) {
    with_lock(Lock) {
        if (!CompressExecutor) {
            CompressExecutor = NYdb::NPersQueue::CreateThreadPoolExecutorAdapter(CompressionPool);
            HandlersExecutor = NYdb::NPersQueue::CreateThreadPoolExecutor(1);
        }
        TWrapperKey key{settings.Server, settings.CredentialsProvider};
        auto it = Wrappers.find(key);
        if (Wrappers.end() == it) {
            CreateWrapper(key, logger);
            it = Wrappers.find(key);
        }
        return *it->second.PersQueueClient;
    }
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateNewConsumer(const TConsumerSettings& settings, TIntrusivePtr<ILogger> logger)
{
    if (Settings.EnableGRpcV1 && settings.Server.UseLogbrokerCDS != EClusterDiscoveryUsageMode::DontUse && !settings.CommitsDisabled && settings.Unpack) {
        auto ret = std::make_shared<TLocalConsumerImplCaller<TYdbSdkCompatibilityConsumer>>(settings,
                                                                                            GetSelfRefsAreDeadEvent(false),
                                                                                            this,
                                                                                            logger,
                                                                                            GetOrCreatePersQueueClient(settings, logger));
        ret->Init();
        AddToDestroySet(ret);
        return ret;
    }
    return nullptr;
}

THolder<IConsumer> TPQLib::CreateConsumer(const TConsumerSettings& settings, TIntrusivePtr<ILogger> logger, bool deprecated) {
    CHECK_PQLIB_ALIVE();
    return MakeHolder<TPublicConsumer>(Impl->CreateConsumer(settings, deprecated, std::move(logger)));
}

THolder<IProcessor> TPQLib::CreateProcessor(const TProcessorSettings& settings, TIntrusivePtr<ILogger> logger, bool deprecated) {
    CHECK_PQLIB_ALIVE();
    return MakeHolder<TPublicProcessor>(Impl->CreateProcessor(settings, deprecated, std::move(logger)));
}

void TPQLib::SetLogger(TIntrusivePtr<ILogger> logger) {
    CHECK_PQLIB_ALIVE();
    Impl->SetLogger(std::move(logger));
}

#undef CHECK_PQLIB_ALIVE

static void DoExecute(std::shared_ptr<grpc::CompletionQueue> cq, std::shared_ptr<std::atomic<bool>> shuttingDown) {
    TThread::SetCurrentThreadName("pqlib_grpc_thr");
    void* tag;
    bool ok;
    while (cq->Next(&tag, &ok)) {
        IQueueEvent* const ev(static_cast<IQueueEvent*>(tag));
        if (shuttingDown->load() || !ev->Execute(ok)) {
            ev->DestroyRequest();
        }
    }
}

TPQLibPrivate::TPQLibPrivate(const TPQLibSettings& settings)
    : Settings(settings)
    , CQ(std::make_shared<grpc::CompletionQueue>())
    , Scheduler(MakeHolder<TScheduler>(this))
    , Logger(settings.DefaultLogger)
{
    Y_VERIFY(Settings.ThreadsCount > 0);
    Y_VERIFY(Settings.GRpcThreads > 0);
    Y_VERIFY(Settings.CompressionPoolThreads > 0);
    CompressionPool = std::make_shared<TThreadPool>();
    CompressionPool->Start(Settings.CompressionPoolThreads);
    QueuePool.Start(Settings.ThreadsCount);
    GRpcThreads.reserve(Settings.GRpcThreads);
    for (size_t i = 0; i < Settings.GRpcThreads; ++i) {
        GRpcThreads.emplace_back([cq = CQ, shuttingDown = ShuttingDown](){ DoExecute(cq, shuttingDown); });
    }
    CreateSelfRefsAreDeadPtr();
}

TPQLibPrivate::~TPQLibPrivate() {
    DEBUG_LOG("Destroying PQLib. Destroying scheduler.", "", "");
    Scheduler.Reset();

    DEBUG_LOG("Destroying PQLib. Set stop flag.", "", "");
    Stop();

    if (CQ) {
        DEBUG_LOG("Destroying PQLib. Shutting down completion queue.", "", "");
        CQ->Shutdown();
    }

    if (!AtomicGet(HasDeprecatedObjects)) {
        DEBUG_LOG("Destroying PQLib. Joining threads.", "", "");
        for (std::thread& thread : GRpcThreads) {
            thread.join();
        }
    } else {
        DEBUG_LOG("Destroying PQLib. Detaching threads.", "", "");
        for (std::thread& thread : GRpcThreads) {
            thread.detach();
        }
    }

    if (YdbDriver) {
        YdbDriver->Stop();
        YdbDriver = nullptr;
    }

    DEBUG_LOG("Destroying PQLib. Stopping compression pool.", "", "");
    CompressionPool->Stop();

    DEBUG_LOG("Destroying PQLib. Stopping queue pool.", "", "");
    QueuePool.Stop();
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateRawProducer(const TProducerSettings& settings, const TChannelInfo& channelInfo, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    std::shared_ptr<IProducerImpl> res;
    auto rawProducerImpl = CreateRawProducerImpl<TProducer>(settings, refsAreDeadPtr, ChooseLogger(logger));
    rawProducerImpl->SetChannel(channelInfo);
    res = std::move(rawProducerImpl);
    return res;
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateRawProducer(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    TChannelHolder t = CreateChannel(settings, ChooseLogger(logger));
    std::shared_ptr<IProducerImpl> res;

    auto rawProducerImpl = CreateRawProducerImpl<TProducer>(settings, refsAreDeadPtr, ChooseLogger(logger));
    rawProducerImpl->SetChannel(t);
    res = std::move(rawProducerImpl);
    return res;
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateProducer(const TProducerSettings& settings, const TChannelInfo& channelInfo, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create producer", settings.SourceId, "");
    std::shared_ptr<IProducerImpl> rawProducer = CreateRawProducer(settings, channelInfo, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    std::shared_ptr<IProducerImpl> compressing = std::make_shared<TLocalProducerImplCaller<TCompressingProducer>>(std::move(rawProducer), settings.Codec, settings.Quality, GetSelfRefsAreDeadEvent(deprecated), this, ChooseLogger(logger));
    compressing->Init();
    if (!deprecated) {
        AddToDestroySet(compressing);
    }
    return compressing;
}

class TLoggerWrapper : public TLogBackend {
public:
    TLoggerWrapper(TIntrusivePtr<ILogger> logger) noexcept
    : Logger(logger)
    {}

    ~TLoggerWrapper() {}

    void WriteData(const TLogRecord& rec)
    {
        Logger->Log(TString(rec.Data, rec.Len), "", "", (int)rec.Priority);
    }

    void ReopenLog() {}

private:
    TIntrusivePtr<ILogger> Logger;
};


void TPQLibPrivate::CreateWrapper(const TWrapperKey& key, TIntrusivePtr<ILogger> logger) {
    auto& wrapper = Wrappers[key];
    const auto& settings = key.Settings;
    if (!YdbDriver) {
        NYdb::TDriverConfig config;

        config.SetEndpoint(TStringBuilder() << settings.Address << ":" << settings.Port) //TODO BAD
              .SetDatabase(settings.Database.empty() ? "/Root" : settings.Database)
              .SetNetworkThreadsNum(Settings.GRpcThreads)
              .SetClientThreadsNum(Settings.ThreadsCount)
              .SetGRpcKeepAliveTimeout(TDuration::Seconds(90))
              .SetGRpcKeepAlivePermitWithoutCalls(true)
              .SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
        if (logger)
            config.SetLog(MakeHolder<TLoggerWrapper>(logger));
        if (settings.UseSecureConnection)
            config.UseSecureConnection(settings.CaCert);

        YdbDriver = MakeHolder<NYdb::TDriver>(config);
    }

    NYdb::NPersQueue::TPersQueueClientSettings pqSettings;
    pqSettings.DefaultCompressionExecutor(CompressExecutor)
              .DefaultHandlersExecutor(HandlersExecutor)
              .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Auto)
              .DiscoveryEndpoint(TStringBuilder() << settings.Address << ":" << settings.Port)
              .Database(settings.Database);
    if (key.Provider) {
        pqSettings.CredentialsProviderFactory(std::shared_ptr<NYdb::ICredentialsProviderFactory>(new TCredentialsProviderFactoryWrapper(key.Provider)));
    }
    wrapper.PersQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*YdbDriver, pqSettings);
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateNewProducer(const TProducerSettings& settings, TIntrusivePtr<ILogger> logger)
{
    if (Settings.EnableGRpcV1) {
        auto ret = std::make_shared<TLocalProducerImplCaller<TYdbSdkCompatibilityProducer>>(settings,
                                                                                            GetOrCreatePersQueueClient(settings, logger),
                                                                                            GetSelfRefsAreDeadEvent(false),
                                                                                            this);
        ret->Init();
        AddToDestroySet(ret);
        return ret;
    }
    return nullptr;
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateProducer(const TProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    auto newP = CreateNewProducer(settings, ChooseLogger(logger));
    if (newP) return newP;

    DEBUG_LOG("Create producer", settings.SourceId, "");
    std::shared_ptr<IProducerImpl> rawProducer = CreateRawProducer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    std::shared_ptr<IProducerImpl> compressing = std::make_shared<TLocalProducerImplCaller<TCompressingProducer>>(std::move(rawProducer), settings.Codec, settings.Quality, GetSelfRefsAreDeadEvent(deprecated), this, ChooseLogger(logger));
    compressing->Init();
    if (!deprecated) {
        AddToDestroySet(compressing);
    }
    return compressing;
}

template<typename TRawProducerImpl>
std::shared_ptr<TRawProducerImpl> TPQLibPrivate::CreateRawProducerImpl(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create raw producer", settings.SourceId, "");
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    NThreading::TPromise<TProducerCreateResponse> promise = NThreading::NewPromise<TProducerCreateResponse>();
    auto ret = std::make_shared<TLocalProducerImplCaller<TRawProducerImpl>>(settings, CQ, promise, std::move(refsAreDeadPtr), this, ChooseLogger(logger));
    ret->Init();
    if (addToDestroySet) {
        AddToDestroySet(ret);
    }
    return ret;
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateRawConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    auto t = CreateChannel(settings.Server, settings.CredentialsProvider, ChooseLogger(logger), settings.PreferLocalProxy);
    auto res = CreateRawConsumerImpl(settings, std::move(refsAreDeadPtr), ChooseLogger(logger));
    res->SetChannel(t);
    return std::move(res);
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateRawConsumer(const TConsumerSettings& settings, const TChannelInfo& channelInfo, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    auto res = CreateRawConsumerImpl(settings, refsAreDeadPtr, ChooseLogger(logger));
    res->SetChannel(channelInfo);
    return std::move(res);
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateRawMultiClusterConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create multicluster consumer", "", "");
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    auto consumer = std::make_shared<TLocalConsumerImplCaller<TMultiClusterConsumer>>(settings, std::move(refsAreDeadPtr), this, logger);
    consumer->Init();
    if (addToDestroySet) {
        AddToDestroySet(consumer);
    }
    return std::move(consumer);
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateDecompressingConsumer(std::shared_ptr<IConsumerImpl> subconsumer, const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger) {
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    std::shared_ptr<IConsumerImpl> consumer = std::make_shared<TLocalConsumerImplCaller<TDecompressingConsumer>>(std::move(subconsumer), settings, std::move(refsAreDeadPtr), this, ChooseLogger(logger));
    consumer->Init();
    if (addToDestroySet) {
        AddToDestroySet(consumer);
    }
    return consumer;
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateRawRetryingConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger) {
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    std::shared_ptr<IConsumerImpl> consumer = std::make_shared<TLocalConsumerImplCaller<TRetryingConsumer>>(settings, std::move(refsAreDeadPtr), this, logger);
    consumer->Init();
    if (addToDestroySet) {
        AddToDestroySet(consumer);
    }
    return consumer;
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateConsumer(const TConsumerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    if (auto newConsumer = CreateNewConsumer(settings, ChooseLogger(logger))) {
        return newConsumer;
    }

    DEBUG_LOG("Create consumer", "", "");
    // Create raw consumer.
    std::shared_ptr<IConsumerImpl> consumer;
    if (settings.ReadFromAllClusterSources) {
        if (settings.ReadMirroredPartitions) {
            ythrow yexception() << "Can't create consumer with both ReadMirroredPartitions and ReadFromAllClusterSources options";
        }
        if (settings.Server.UseLogbrokerCDS == EClusterDiscoveryUsageMode::DontUse) {
            ythrow yexception() << "Can't create consumer with ReadFromAllClusterSources option, but without using cluster discovery";
        }
        consumer = CreateRawMultiClusterConsumer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    } else if (settings.ReconnectOnFailure) {
        consumer = CreateRawRetryingConsumer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    } else {
        consumer = CreateRawConsumer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    }
    if (settings.Unpack) {
        consumer = CreateDecompressingConsumer(std::move(consumer), settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    }
    return consumer;
}

std::shared_ptr<IConsumerImpl> TPQLibPrivate::CreateConsumer(const TConsumerSettings& settings, const TChannelInfo& channelInfo, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create consumer", "", "");
    std::shared_ptr<IConsumerImpl> consumer = CreateRawConsumer(settings, channelInfo, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    if (settings.Unpack) {
        return CreateDecompressingConsumer(std::move(consumer), settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    }
    return consumer;
}

std::shared_ptr<TConsumer> TPQLibPrivate::CreateRawConsumerImpl(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create raw consumer", "", "");
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    NThreading::TPromise<TConsumerCreateResponse> promise = NThreading::NewPromise<TConsumerCreateResponse>();
    std::shared_ptr<TConsumer> consumer = std::make_shared<TLocalConsumerImplCaller<TConsumer>>(settings, CQ, promise, std::move(refsAreDeadPtr), this, ChooseLogger(logger));
    consumer->Init();
    if (addToDestroySet) {
        AddToDestroySet(consumer);
    }
    return consumer;
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateRawRetryingProducer(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create raw retrying producer", settings.SourceId, "");
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    auto producer = std::make_shared<TLocalProducerImplCaller<TRetryingProducer>>(settings, std::move(refsAreDeadPtr), this, ChooseLogger(logger));
    producer->Init();
    if (addToDestroySet) {
        AddToDestroySet(producer);
    }
    return std::move(producer);
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateRetryingProducer(const TProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    auto newP = CreateNewProducer(settings, ChooseLogger(logger));
    if (newP) return newP;

    DEBUG_LOG("Create retrying producer", settings.SourceId, "");
    std::shared_ptr<IProducerImpl> rawProducer = CreateRawRetryingProducer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    std::shared_ptr<IProducerImpl> compressing = std::make_shared<TLocalProducerImplCaller<TCompressingProducer>>(std::move(rawProducer), settings.Codec, settings.Quality, GetSelfRefsAreDeadEvent(deprecated), this, ChooseLogger(logger));
    compressing->Init();
    if (!deprecated) {
        AddToDestroySet(compressing);
    }
    return compressing;
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateRawMultiClusterProducer(const TMultiClusterProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create raw multicluster producer", settings.SourceIdPrefix, "");
    const bool addToDestroySet = refsAreDeadPtr != nullptr;
    auto producer = std::make_shared<TLocalProducerImplCaller<TMultiClusterProducer>>(settings, std::move(refsAreDeadPtr), this, ChooseLogger(logger));
    producer->Init();
    if (addToDestroySet) {
        AddToDestroySet(producer);
    }
    return std::move(producer);
}

std::shared_ptr<IProducerImpl> TPQLibPrivate::CreateMultiClusterProducer(const TMultiClusterProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create multicluster producer", settings.SourceIdPrefix, "");
    if (settings.ServerWeights.empty()) {
        ythrow yexception() << "Can't create producer without server";
    }
    const ECodec codec = settings.ServerWeights[0].ProducerSettings.Codec;
    const int quality = settings.ServerWeights[0].ProducerSettings.Quality;
    for (const auto& w : settings.ServerWeights) {
        if (w.ProducerSettings.Codec != codec) {
            ythrow yexception() << "Can't create multicluster producer with different codecs";
        }
    }

    std::shared_ptr<IProducerImpl> rawProducer = CreateRawMultiClusterProducer(settings, GetSelfRefsAreDeadEvent(deprecated), ChooseLogger(logger));
    std::shared_ptr<IProducerImpl> compressing = std::make_shared<TLocalProducerImplCaller<TCompressingProducer>>(std::move(rawProducer), codec, quality, GetSelfRefsAreDeadEvent(deprecated), this, ChooseLogger(logger));
    compressing->Init();
    if (!deprecated) {
        AddToDestroySet(compressing);
    }
    return compressing;
}

std::shared_ptr<IProcessorImpl> TPQLibPrivate::CreateProcessor(const TProcessorSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger)
{
    DEBUG_LOG("Create processor", "", "");
    auto processor = std::make_shared<TLocalProcessorImplCaller<TProcessor>>(settings, GetSelfRefsAreDeadEvent(deprecated), this, ChooseLogger(logger));
    processor->Init();
    if (!deprecated) {
        AddToDestroySet(processor);
    }
    return processor;
}

TChannelHolder TPQLibPrivate::CreateChannel(
        const TServerSetting& server, const TCredProviderPtr& credentialsProvider, TIntrusivePtr<ILogger> logger,
        bool preferLocalProxy
) {
    DEBUG_LOG("Create channel", "", "");
    TChannelHolder res;
    res.ChannelPtr = new TChannel(server, credentialsProvider, this, ChooseLogger(logger), preferLocalProxy);
    res.ChannelInfo = res.ChannelPtr->GetChannel();
    res.ChannelPtr->Start();
    return res;
}


TChannelHolder TPQLibPrivate::CreateChannel(
        const TProducerSettings& settings, TIntrusivePtr<ILogger> logger, bool preferLocalProxy
) {
    DEBUG_LOG("Create channel", "", "");
    TChannelHolder res;
    res.ChannelPtr = new TChannel(settings, this, ChooseLogger(logger), preferLocalProxy);
    res.ChannelInfo = res.ChannelPtr->GetChannel();
    res.ChannelPtr->Start();
    return res;
}

void TPQLibPrivate::CancelObjectsAndWait() {
    {
        auto guard = Guard(Lock);
        DEBUG_LOG("Destroying PQLib. Cancelling objects: " << SyncDestroyedSet.size(), "", "");
        for (auto& ptr : SyncDestroyedSet) {
            auto ref = ptr.lock();
            if (ref) {
                ref->Cancel();
            }
        }
        SelfRefsAreDeadPtr = nullptr;
    }
    DEBUG_LOG("Destroying PQLib. Waiting refs to die", "", "");
    SelfRefsAreDeadEvent.Wait();

    DEBUG_LOG("Destroying PQLib. HasDeprecatedObjects: " << AtomicGet(HasDeprecatedObjects) << ". Refs to PQLibPrivate: " << RefCount(), "", "");
    if (!AtomicGet(HasDeprecatedObjects)) {
        Y_ASSERT(RefCount() == 1);
    }
}

void TPQLibPrivate::CreateSelfRefsAreDeadPtr() {
    void* fakeAddress = &SelfRefsAreDeadEvent;
    SelfRefsAreDeadPtr = std::shared_ptr<void>(fakeAddress,
                                               [event = SelfRefsAreDeadEvent](void*) mutable {
                                                   event.Signal();
                                               });
}

void TPQLibPrivate::AddToDestroySet(std::weak_ptr<TSyncDestroyed> obj) {
    auto guard = Guard(Lock);
    // Check dead objects first
    if (DestroySetAddCounter * 2 > SyncDestroyedSet.size()) {
        for (auto i = SyncDestroyedSet.begin(); i != SyncDestroyedSet.end();) {
            if (i->expired()) {
                i = SyncDestroyedSet.erase(i);
            } else {
                ++i;
            }
        }
        DestroySetAddCounter = 0;
    }
    ++DestroySetAddCounter;
    SyncDestroyedSet.emplace(std::move(obj));
}

} // namespace NPersQueue
