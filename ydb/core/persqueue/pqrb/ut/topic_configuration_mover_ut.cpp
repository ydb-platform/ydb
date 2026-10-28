#include <ydb/core/persqueue/pqrb/topic_configuration_mover.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

class TTestFactory : public TPersQueueMirrorReaderFactory {
public:
    mutable ui32 CredentialsRequests = 0;
    mutable ui32 ClientAttempts = 0;
    mutable ui32 ClientsCreated = 0;
    mutable ui32 CredentialFailures = 0;
    mutable ui32 ClientFailures = 0;
    bool DeferCredentials = false;
    NThreading::TPromise<NYdb::TCredentialsProviderFactoryPtr> CredentialsPromise =
        NThreading::NewPromise<NYdb::TCredentialsProviderFactoryPtr>();
    mutable NKikimrPQ::TMirrorPartitionConfig ClientConfig;
    mutable NYdb::TCredentialsProviderFactoryPtr ClientCredentials;

    TTestFactory() {
        Driver = std::make_shared<NYdb::TDriver>(NYdb::TDriverConfig());
    }

    NYdb::NTopic::TTopicClient GetTopicClient(const NKikimrPQ::TMirrorPartitionConfig& config,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentials) const override
    {
        ++ClientAttempts;
        if (ClientFailures) {
            --ClientFailures;
            ythrow yexception() << "client creation failed";
        }
        ClientConfig = config;
        ClientCredentials = credentials;
        auto client = IPersQueueMirrorReaderFactory::GetTopicClient(config, std::move(credentials));
        ++ClientsCreated;
        return client;
    }

protected:
    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials) const override
    {
        ++CredentialsRequests;
        if (CredentialFailures) {
            --CredentialFailures;
            ythrow yexception() << "credentials creation failed";
        }
        if (DeferCredentials) {
            return CredentialsPromise.GetFuture();
        }
        return TPersQueueMirrorReaderFactory::GetCredentialsProviderImpl(credentials);
    }
};

struct TDescribeRequests {
    TVector<NThreading::TPromise<NYdb::NTopic::TDescribeTopicResult>> Topics;
    struct TConsumerRequest {
        TString Name;
        NThreading::TPromise<NYdb::NTopic::TDescribeConsumerResult> Promise;
    };
    TVector<TConsumerRequest> Consumers;
};

class TTestMover : public TTopicConfigurationMover {
public:
    TTestMover(const TActorId& parent, const NKikimrPQ::TMirrorPartitionConfig& config, TDescribeRequests& requests)
        : TTopicConfigurationMover(42, parent, config)
        , Requests(requests)
    {}

protected:
    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic() override {
        auto promise = NThreading::NewPromise<NYdb::NTopic::TDescribeTopicResult>();
        Requests.Topics.push_back(promise);
        return promise.GetFuture();
    }

    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& name) override {
        auto promise = NThreading::NewPromise<NYdb::NTopic::TDescribeConsumerResult>();
        Requests.Consumers.push_back({name, promise});
        return promise.GetFuture();
    }

private:
    TDescribeRequests& Requests;
};

NYdb::NTopic::TDescribeTopicResult MakeTopic(size_t consumers, NYdb::EStatus status = NYdb::EStatus::SUCCESS, bool stats = true) {
    Ydb::Topic::DescribeTopicResult result;
    for (ui32 partition : {0, 7}) {
        auto* info = result.add_partitions();
        info->set_partition_id(partition);
        if (stats) {
            auto* offsets = info->mutable_partition_stats()->mutable_partition_offsets();
            offsets->set_start(partition * 100 + 10);
            offsets->set_end(partition * 100 + 10000);
        }
    }
    for (size_t i = 0; i < consumers; ++i) {
        result.add_consumers()->set_name(TStringBuilder() << "consumer-" << i);
    }
    return {NYdb::TStatus(status, NYdb::NIssue::TIssues()), std::move(result)};
}

NYdb::NTopic::TDescribeConsumerResult MakeConsumer(const TString& name, ui64 offset, bool stats = true,
    NYdb::EStatus status = NYdb::EStatus::SUCCESS)
{
    Ydb::Topic::DescribeConsumerResult result;
    result.mutable_consumer()->set_name(name);
    for (ui32 partition : {0, 7}) {
        auto* info = result.add_partitions();
        info->set_partition_id(partition);
        if (stats) {
            info->mutable_partition_stats();
            info->mutable_partition_consumer_stats()->set_committed_offset(offset + partition);
        }
    }
    return {NYdb::TStatus(status, NYdb::NIssue::TIssues()), std::move(result)};
}

struct TEnv {
    TTestFactory Factory;
    TDescribeRequests Requests;
    TTestBasicRuntime Runtime;
    TActorId Parent;
    TActorId Mover;

    TEnv(bool deferCredentials = false, ui32 credentialFailures = 0, ui32 clientFailures = 0)
        : Runtime(1, false)
    {
        TAppPrepare prepare;
        Runtime.Initialize(prepare.Unwrap());
        Factory.DeferCredentials = deferCredentials;
        Factory.CredentialFailures = credentialFailures;
        Factory.ClientFailures = clientFailures;
        Runtime.GetAppData(0).PersQueueMirrorReaderFactory = &Factory;
        Parent = Runtime.AllocateEdgeActor();

        NKikimrPQ::TMirrorPartitionConfig config;
        config.SetEndpoint("localhost");
        config.SetEndpointPort(1);
        config.SetDatabase("/Root/source");
        config.SetTopic("source-topic");
        Mover = Runtime.Register(new TTestMover(Parent, config, Requests));
        Runtime.EnableScheduleForActor(Mover);
    }

    void WaitFor(const std::function<bool()>& condition) {
        if (condition()) {
            return;
        }
        TDispatchOptions options;
        options.CustomFinalCondition = condition;
        UNIT_ASSERT(Runtime.DispatchEvents(options, TDuration::Seconds(10)));
        UNIT_ASSERT(condition());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTopicConfigurationMoverTests) {

Y_UNIT_TEST(WaitsForCredentialsBeforeCreatingClient) {
    TEnv env(true);
    env.WaitFor([&] { return env.Factory.CredentialsRequests == 1; });
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientsCreated, 0);
    env.Runtime.Send(new IEventHandle(env.Mover, env.Parent, new TEvPQ::TEvInitCredentials()));
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.CredentialsRequests, 1);

    auto credentials = NYdb::CreateInsecureCredentialsProviderFactory();
    env.Factory.CredentialsPromise.SetValue(credentials);
    env.WaitFor([&] { return env.Factory.ClientsCreated == 1; });
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientConfig.GetEndpoint(), "localhost");
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientConfig.GetEndpointPort(), 1);
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientConfig.GetDatabase(), "/Root/source");
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientConfig.GetTopic(), "source-topic");
    UNIT_ASSERT(env.Factory.ClientCredentials == credentials);
}

Y_UNIT_TEST(RetriesCredentialsAndClientCreation) {
    TEnv env(false, 1, 1);
    env.WaitFor([&] { return env.Factory.ClientsCreated == 1; });
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.CredentialsRequests, 3);
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientAttempts, 2);
}

Y_UNIT_TEST(CanStopWhileWaitingForCredentials) {
    TEnv env(true);
    env.WaitFor([&] { return env.Factory.CredentialsRequests == 1; });
    env.Runtime.Send(new IEventHandle(env.Mover, env.Parent, new TEvents::TEvPoisonPill()));
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    env.Factory.CredentialsPromise.SetValue(NYdb::CreateInsecureCredentialsProviderFactory());
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.ClientsCreated, 0);
}

Y_UNIT_TEST(DescribesConsumersWithFiveRequestsInFlightAndGroupsOffsetsByPartition) {
    TEnv env;
    env.WaitFor([&] { return env.Requests.Topics.size() == 1; });
    env.Requests.Topics[0].SetValue(MakeTopic(12));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 5; });
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(env.Requests.Consumers.size(), 5);

    // An out-of-order completion must immediately release a slot for the sixth consumer.
    env.Requests.Consumers[4].Promise.SetValue(MakeConsumer("consumer-4", 400));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 6; });
    size_t completed = 1;
    for (size_t i = 0; i < 12; ++i) {
        if (i == 4) {
            continue;
        }
        UNIT_ASSERT(i < env.Requests.Consumers.size());
        const auto name = env.Requests.Consumers[i].Name;
        UNIT_ASSERT_VALUES_EQUAL(name, TStringBuilder() << "consumer-" << i);
        env.Requests.Consumers[i].Promise.SetValue(MakeConsumer(name, i * 100));
        ++completed;
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.Requests.Consumers.size(), Min(size_t(12), completed + 5));
    }
    THashSet<ui32> received;
    for (size_t i = 0; i < 2; ++i) {
        auto event = env.Runtime.GrabEdgeEvent<TEvPQ::TEvPartitionConsumerOffsets>(env.Parent, TDuration::Seconds(1));
        UNIT_ASSERT(event);
        const auto& record = event->Get()->Record;
        UNIT_ASSERT(received.insert(record.GetPartitionId()).second);
        UNIT_ASSERT_VALUES_EQUAL(record.ConsumerOffsetsSize(), 12);
        UNIT_ASSERT(record.HasStartOffset());
        UNIT_ASSERT(record.HasEndOffset());
        UNIT_ASSERT_VALUES_EQUAL(record.GetStartOffset(), record.GetPartitionId() * 100 + 10);
        UNIT_ASSERT_VALUES_EQUAL(record.GetEndOffset(), record.GetPartitionId() * 100 + 10000);
        THashMap<TString, ui64> offsets;
        for (const auto& offset : record.GetConsumerOffsets()) {
            UNIT_ASSERT(offsets.emplace(offset.GetConsumer(), offset.GetCommittedOffset()).second);
        }
        for (size_t consumer = 0; consumer < 12; ++consumer) {
            UNIT_ASSERT_VALUES_EQUAL(offsets.at(TStringBuilder() << "consumer-" << consumer), consumer * 100 + record.GetPartitionId());
        }
    }
    UNIT_ASSERT(received.contains(0) && received.contains(7));
    UNIT_ASSERT_VALUES_EQUAL(env.Requests.Topics.size(), 1);
}

Y_UNIT_TEST(RetriesFailedDescriptionsWithoutPublishingPartialOffsets) {
    TEnv env;
    env.WaitFor([&] { return env.Requests.Topics.size() == 1; });
    env.Requests.Topics[0].SetValue(MakeTopic(2, NYdb::EStatus::UNAVAILABLE));
    env.WaitFor([&] { return env.Requests.Topics.size() == 2; });
    env.Requests.Topics[1].SetException(std::make_exception_ptr(yexception() << "transport error"));
    env.WaitFor([&] { return env.Requests.Topics.size() == 3; });
    env.Requests.Topics[2].SetValue(MakeTopic(2, NYdb::EStatus::SUCCESS, false));
    env.WaitFor([&] { return env.Requests.Topics.size() == 4; });
    UNIT_ASSERT(env.Requests.Consumers.empty());
    env.Requests.Topics[3].SetValue(MakeTopic(2));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 2; });
    env.Requests.Consumers[0].Promise.SetValue(MakeConsumer("consumer-0", 100));
    env.Requests.Consumers[1].Promise.SetValue(MakeConsumer("consumer-1", 200, false));
    UNIT_ASSERT(!env.Runtime.GrabEdgeEvent<TEvPQ::TEvPartitionConsumerOffsets>(env.Parent, TDuration::MilliSeconds(50)));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 3; });
    UNIT_ASSERT_VALUES_EQUAL(env.Requests.Consumers[2].Name, "consumer-1");
    env.Requests.Consumers[2].Promise.SetValue(MakeConsumer("consumer-1", 200, true, NYdb::EStatus::UNAVAILABLE));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 4; });
    env.Requests.Consumers[3].Promise.SetException(std::make_exception_ptr(yexception() << "transport error"));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 5; });
    env.Requests.Consumers[4].Promise.SetValue(MakeConsumer("consumer-1", 200));
    for (size_t i = 0; i < 2; ++i) {
        auto event = env.Runtime.GrabEdgeEvent<TEvPQ::TEvPartitionConsumerOffsets>(env.Parent, TDuration::Seconds(1));
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Record.ConsumerOffsetsSize(), 2);
    }
    UNIT_ASSERT_VALUES_EQUAL(env.Requests.Topics.size(), 4);
}

Y_UNIT_TEST(TopicWithoutConsumersSendsEmptyOffsets) {
    TEnv env;
    env.WaitFor([&] { return env.Requests.Topics.size() == 1; });
    env.Requests.Topics[0].SetValue(MakeTopic(0));
    for (size_t i = 0; i < 2; ++i) {
        auto event = env.Runtime.GrabEdgeEvent<TEvPQ::TEvPartitionConsumerOffsets>(env.Parent, TDuration::Seconds(1));
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Record.ConsumerOffsetsSize(), 0);
    }
    UNIT_ASSERT(env.Requests.Consumers.empty());
}

Y_UNIT_TEST(CanStopWithConsumerFuturesPending) {
    TEnv env;
    size_t sentOffsets = 0;
    env.Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvPQ::TEvPartitionConsumerOffsets::EventType) {
            ++sentOffsets;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    env.WaitFor([&] { return env.Requests.Topics.size() == 1; });
    env.Requests.Topics[0].SetValue(MakeTopic(6));
    env.WaitFor([&] { return env.Requests.Consumers.size() == 5; });
    env.Runtime.Send(new IEventHandle(env.Mover, env.Parent, new TEvents::TEvPoisonPill()));
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    for (auto& request : env.Requests.Consumers) {
        request.Promise.SetValue(MakeConsumer(request.Name, 123));
    }
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(50));
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets, 0);
    UNIT_ASSERT_VALUES_EQUAL(env.Requests.Consumers.size(), 5);
    env.Runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
}

Y_UNIT_TEST(WaitsForEveryAckAndRetriesOnlyPendingPartitions) {
    TEnv env;
    TVector<std::pair<ui32, ui64>> sentOffsets;
    size_t completions = 0;
    env.Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvPQ::TEvPartitionConsumerOffsets::EventType) {
            sentOffsets.emplace_back(event->Get<TEvPQ::TEvPartitionConsumerOffsets>()->Record.GetPartitionId(), event->Cookie);
            return TTestActorRuntimeBase::EEventAction::DROP;
        } else if (event->GetTypeRewrite() == TEvPQ::TEvTopicConfigurationMovingFinished::EventType) {
            ++completions;
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    env.WaitFor([&] { return env.Requests.Topics.size() == 1; });
    env.Requests.Topics[0].SetValue(MakeTopic(0));
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(completions, 0);
    auto ack = [&](ui32 partition, ui64 cookie) {
        auto event = MakeHolder<TEvPQ::TEvPartitionConsumerOffsetsAck>();
        event->Record.SetPartitionId(partition);
        env.Runtime.Send(new IEventHandle(env.Mover, env.Parent, event.Release(), 0, cookie), 0, true);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    };
    ack(0, 1);
    ack(0, 1); // Duplicate.
    ack(42, 43); // Unknown partition.
    ack(7, 999); // Wrong cookie.
    UNIT_ASSERT_VALUES_EQUAL(completions, 0);
    env.Runtime.SimulateSleep(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets.back().first, 7);
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets.back().second, 8);
    ack(7, 8);
    UNIT_ASSERT_VALUES_EQUAL(completions, 1);
    ack(7, 8);
    UNIT_ASSERT_VALUES_EQUAL(completions, 1);
    env.Runtime.SimulateSleep(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(completions, 2);
    UNIT_ASSERT_VALUES_EQUAL(sentOffsets.size(), 3);
    env.Runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
}

} // Y_UNIT_TEST_SUITE(TTopicConfigurationMoverTests)
} // namespace NKikimr::NPQ
