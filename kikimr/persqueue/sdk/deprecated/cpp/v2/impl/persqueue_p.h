#pragma once

#include "channel.h"
#include "internals.h"
#include "iconsumer_p.h"
#include "iprocessor_p.h"
#include "iproducer_p.h"
#include "scheduler.h"
#include "queue_pool.h"

#include <kikimr/public/sdk/cpp/client/ydb_persqueue/persqueue.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iprocessor.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iconsumer.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/queue.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>
#include <util/thread/pool.h>

#include <util/datetime/base.h>

#include <grpc++/create_channel.h>
#include <grpc++/completion_queue.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <type_traits>
#include <memory>
#include <set>
#include <thread>

namespace NPersQueue {

class TProducer;
class TConsumer;


struct TWrapperKey {
    TServerSetting Settings;
    std::shared_ptr<ICredentialsProvider> Provider;

    bool operator <(const TWrapperKey& b) const {
        return Settings < b.Settings || (!(b.Settings <  Settings) && Provider < b.Provider);
    }

};



class TCredentialsProviderWrapper : public NYdb::ICredentialsProvider {
public:
    TCredentialsProviderWrapper(std::shared_ptr<NPersQueue::ICredentialsProvider> provider)
    : Provider(provider)
    {}

    bool IsValid() const override {
        return true;
    }

    TString GetAuthInfo() const override {
        return GetToken(Provider.get());
    }

    private:
        std::shared_ptr<NPersQueue::ICredentialsProvider> Provider;
};


class TCredentialsProviderFactoryWrapper : public NYdb::ICredentialsProviderFactory {
public:
    TString GetClientIdentity() const override {
        return Guid;
    }

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return Provider;
    };


    TCredentialsProviderFactoryWrapper(std::shared_ptr<NPersQueue::ICredentialsProvider> provider)
        : Provider(new TCredentialsProviderWrapper(provider))
        , Guid(CreateGuidAsString())
    {}

private:
    std::shared_ptr<NYdb::ICredentialsProvider> Provider;
    TString Guid;
};

class TPQLibPrivate: public TAtomicRefCount<TPQLibPrivate> {
    template <class T>
    class TBaseFutureSubscriberObjectInQueue: public IObjectInQueue {
    public:
        void SetFuture(const NThreading::TFuture<T>& future) {
            Future = future;
        }

    protected:
        NThreading::TFuture<T> Future;
    };

    template <class T, class TFunc>
    class TFutureSubscriberObjectInQueue: public TBaseFutureSubscriberObjectInQueue<T> {
    public:
        template <class TF>
        TFutureSubscriberObjectInQueue(TF&& f)
            : Func(std::forward<TF>(f))
        {
        }

        void Process(void*) override {
            THolder<IObjectInQueue> holder(this); // like in TOwnedObjectInQueue::Process()
            Func(this->Future);
        }

    private:
        TFunc Func;
    };

    template <class T, class TFunc>
    static THolder<TBaseFutureSubscriberObjectInQueue<T>> MakeFutureSubscribeObjectInQueue(TFunc&& f) {
        return MakeHolder<TFutureSubscriberObjectInQueue<T, std::remove_reference_t<TFunc>>>(std::forward<TFunc>(f));
    }

    template <class T>
    class TSubscriberFunc {
    public:
        TSubscriberFunc(TPQLibPrivate* pqLib, const void* queueTag, THolder<TBaseFutureSubscriberObjectInQueue<T>> callback)
            : PQLib(pqLib)
            , QueueTag(queueTag)
            , Callback(std::make_shared<THolder<TBaseFutureSubscriberObjectInQueue<T>>>(std::move(callback)))
        {
        }

        void operator()(const NThreading::TFuture<T>& future) {
            if (Callback) {
                (*Callback)->SetFuture(future);
                THolder<IObjectInQueue> callback(std::move(*Callback));
                const bool added = PQLib->GetQueuePool().GetQueue(QueueTag).Add(callback.Get());
                if (added) { // can be false on shutdown
                    Y_UNUSED(callback.Release());
                }
            }
        }

    private:
        TPQLibPrivate* PQLib;
        const void* QueueTag;
        // std::shared_ptr is only because copy constructor is required
        std::shared_ptr<THolder<TBaseFutureSubscriberObjectInQueue<T>>> Callback;
    };

public:
    TPQLibPrivate(const TPQLibSettings& settings);
    TPQLibPrivate(const TPQLibPrivate&) = delete;
    TPQLibPrivate(TPQLibPrivate&&) = delete;
    ~TPQLibPrivate();

    TChannelHolder CreateChannel(const TServerSetting& server, const TCredProviderPtr& credentialsProvider,
                                 TIntrusivePtr<ILogger> logger, bool preferLocalProxy = false);
    TChannelHolder CreateChannel(const TProducerSettings& settings, TIntrusivePtr<ILogger> logger,
                                 bool preferLocalProxy = false);


    std::shared_ptr<IProducerImpl> CreateRawProducer(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IProducerImpl> CreateRawProducer(const TProducerSettings& settings, const TChannelInfo& channelInfo, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IProducerImpl> CreateProducer(const TProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IProducerImpl> CreateProducer(const TProducerSettings& settings, const TChannelInfo& channelInfo, bool deprecated, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IProducerImpl> CreateNewProducer(const TProducerSettings& settings, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IProducerImpl> CreateRawRetryingProducer(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IProducerImpl> CreateRetryingProducer(const TProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IProducerImpl> CreateRawMultiClusterProducer(const TMultiClusterProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IProducerImpl> CreateMultiClusterProducer(const TMultiClusterProducerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IConsumerImpl> CreateRawConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IConsumerImpl> CreateRawConsumer(const TConsumerSettings& settings, const TChannelInfo& channelInfo, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IConsumerImpl> CreateRawRetryingConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IConsumerImpl> CreateRawMultiClusterConsumer(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IConsumerImpl> CreateConsumer(const TConsumerSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IConsumerImpl> CreateConsumer(const TConsumerSettings& settings, const TChannelInfo& channelInfo, bool deprecated, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IConsumerImpl> CreateNewConsumer(const TConsumerSettings& settings, TIntrusivePtr<ILogger> logger);

    std::shared_ptr<IProcessorImpl> CreateProcessor(const TProcessorSettings& settings, bool deprecated, TIntrusivePtr<ILogger> logger);

    TString GetUserAgent() const {
        auto guard = Guard(UALock);
        return UserAgent;
    }

    void SetUserAgent(const TString& userAgent) {
        auto guard = Guard(UALock);
        UserAgent = userAgent;
    }

    void Stop() {
        ShuttingDown->store(true);
    }

    const std::shared_ptr<grpc::CompletionQueue>& GetCompletionQueue() const {
        return CQ;
    }

    const TPQLibSettings& GetSettings() const {
        return Settings;
    }

    TScheduler& GetScheduler() {
        return *Scheduler;
    }

    TQueuePool& GetQueuePool() {
        return QueuePool;
    }

    IThreadPool& GetCompressionPool() {
        return *CompressionPool;
    }

    template <class TFunc, class T>
    void Subscribe(const NThreading::TFuture<T>& future, const void* queueTag, TFunc&& func) {
        TSubscriberFunc<T> f(this, queueTag, MakeFutureSubscribeObjectInQueue<T>(std::forward<TFunc>(func)));
        future.Subscribe(std::move(f));
    }

    void SetLogger(TIntrusivePtr<ILogger> logger) {
        Logger = std::move(logger);
    }

    TIntrusivePtr<ILogger> ChooseLogger(TIntrusivePtr<ILogger> logger) {
        return logger ? std::move(logger) : Logger;
    }

    void CancelObjectsAndWait();

    // for testing
    std::shared_ptr<void> GetSelfRefsAreDeadPtr() const {
        return SelfRefsAreDeadPtr;
    }

    void AddToDestroySet(std::weak_ptr<TSyncDestroyed> obj);

private:
    template<typename TRawProducerImpl>
    std::shared_ptr<TRawProducerImpl> CreateRawProducerImpl(const TProducerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<TConsumer> CreateRawConsumerImpl(const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);
    std::shared_ptr<IConsumerImpl> CreateDecompressingConsumer(std::shared_ptr<IConsumerImpl> subconsumer, const TConsumerSettings& settings, std::shared_ptr<void> refsAreDeadPtr, TIntrusivePtr<ILogger> logger);

    void CreateWrapper(const TWrapperKey& key, TIntrusivePtr<ILogger> logger);

    template <class TConsumerOrProducerSettings>
    NYdb::NPersQueue::TPersQueueClient& GetOrCreatePersQueueClient(const TConsumerOrProducerSettings& settings, const TIntrusivePtr<ILogger>& logger);

    std::shared_ptr<void> GetSelfRefsAreDeadEvent(bool isObjectDeprecated) {
        if (isObjectDeprecated) {
            AtomicSet(HasDeprecatedObjects, 1);
        }
        return isObjectDeprecated ? nullptr : SelfRefsAreDeadPtr;
    }

    void CreateSelfRefsAreDeadPtr();

    using IThreadRef = TAutoPtr<IThreadFactory::IThread>;

private:
    const TPQLibSettings Settings;

    std::shared_ptr<grpc::CompletionQueue> CQ;
    std::vector<std::thread> GRpcThreads;

    std::shared_ptr<std::atomic<bool>> ShuttingDown = std::make_shared<std::atomic<bool>>(false); // shared_ptr is for deprecated case when we have leaked threads

    THolder<TScheduler> Scheduler;
    TQueuePool QueuePool;
    std::shared_ptr<IThreadPool> CompressionPool;

    TIntrusivePtr<ILogger> Logger;

    TAtomic HasDeprecatedObjects = 0;

    TAutoEvent SelfRefsAreDeadEvent;
    std::shared_ptr<void> SelfRefsAreDeadPtr;
    TAdaptiveLock Lock, UALock;
    size_t DestroySetAddCounter = 0;
    std::set<std::weak_ptr<TSyncDestroyed>, std::owner_less<std::weak_ptr<TSyncDestroyed>>> SyncDestroyedSet;

    TString UserAgent = "C++ pqlib v0.3";

    struct TYdbWrapper {
        THolder<NYdb::NPersQueue::TPersQueueClient> PersQueueClient;
    };

    THolder<NYdb::TDriver> YdbDriver;
    std::map<TWrapperKey, TYdbWrapper> Wrappers;
    NYdb::NPersQueue::IExecutor::TPtr CompressExecutor;
    NYdb::NPersQueue::IExecutor::TPtr HandlersExecutor;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactoryWrapper;
};

}
