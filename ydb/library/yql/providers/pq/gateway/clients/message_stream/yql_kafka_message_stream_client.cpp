#include "yql_kafka_message_stream_client.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>

#include <contrib/libs/librdkafka/src-cpp/rdkafkacpp.h>
#include <contrib/libs/librdkafka/src/rdkafka.h>
#include <util/generic/guid.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

namespace NFq {
namespace {

using namespace NActors;

EMessageStreamStatus ConvertError(RdKafka::ErrorCode error) {
    switch (error) {
        case RdKafka::ERR_NO_ERROR:
            return EMessageStreamStatus::Success;
        case RdKafka::ERR_UNKNOWN_TOPIC_OR_PART:
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            return EMessageStreamStatus::NotFound;
        case RdKafka::ERR_TOPIC_AUTHORIZATION_FAILED:
        case RdKafka::ERR_GROUP_AUTHORIZATION_FAILED:
        case RdKafka::ERR__AUTHENTICATION:
            return EMessageStreamStatus::Unauthorized;
        case RdKafka::ERR__TIMED_OUT:
            return EMessageStreamStatus::Timeout;
        default:
            return EMessageStreamStatus::External;
    }
}

void Check(RdKafka::ErrorCode error, TStringBuf operation) {
    if (error != RdKafka::ERR_NO_ERROR) {
        ythrow TMessageStreamException(ConvertError(error)) << "Kafka " << operation << ": " << RdKafka::err2str(error);
    }
}

void Set(RdKafka::Conf& config, const char* name, const std::string& value) {
    std::string error;
    if (config.set(name, value, error) != RdKafka::Conf::CONF_OK) {
        // Do not include configuration values: some of them contain credentials.
        ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Invalid Kafka setting: " << name;
    }
}

std::unique_ptr<RdKafka::Conf> MakeConfig(const TKafkaMessageStreamClientSettings& settings) {
    auto config = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    Set(*config, "bootstrap.servers", std::string(settings.BootstrapBrokers));
    Set(*config, "allow.auto.create.topics", "false");
    Set(*config, "enable.metrics.push", "false");
    const auto token = NYql::CreateStructuredTokenParser(settings.Token);
    if (token.HasBasicAuth()) {
        TString login, password;
        if (!token.GetBasicAuth(login, password) || login.empty() || password.empty()) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Kafka BASIC credentials must be resolved and nonempty";
        }
        if (settings.SaslMechanism != "PLAIN") {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported) << "Unsupported Kafka SASL mechanism";
        }
        Set(*config, "security.protocol", settings.UseTls ? "SASL_SSL" : "SASL_PLAINTEXT");
        Set(*config, "sasl.mechanisms", std::string(settings.SaslMechanism));
        Set(*config, "sasl.username", std::string(login));
        Set(*config, "sasl.password", std::string(password));
    } else {
        if (!settings.Token.empty() && !token.IsNoAuth()) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported) << "Kafka supports NONE and BASIC authentication";
        }
        Set(*config, "security.protocol", settings.UseTls ? "SSL" : "PLAINTEXT");
    }
    return config;
}

class TKafkaReadSession;

struct TEvKafkaPoll: TEventLocal<TEvKafkaPoll, EventSpaceBegin(TEvents::ES_PRIVATE) + 31> {};

struct TEvKafkaCommand: TEventLocal<TEvKafkaCommand, EventSpaceBegin(TEvents::ES_PRIVATE) + 32> {
    explicit TEvKafkaCommand(std::function<void(TKafkaReadSession&)> command)
        : Command(std::move(command))
    {
    }

    std::function<void(TKafkaReadSession&)> Command;
};

class TKafkaPartitionControl final: public IMessageStreamPartitionControl, public std::enable_shared_from_this<TKafkaPartitionControl> {
public:
    TKafkaPartitionControl(std::weak_ptr<TKafkaReadSession> session, ui64 id)
        : Session(std::move(session))
        , Id{ id }
    {
    }

    TMessageStreamPartitionId GetPartitionId() const override {
        return Id;
    }

    void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) override;

    void ConfirmStop() override {
    }

    void ConfirmExhausted() override {
    }

    void RequestStatus() override;
    bool AcknowledgeRange(ui64 start, ui64 end) override;

    const std::weak_ptr<TKafkaReadSession> Session;
    const TMessageStreamPartitionId Id;
    std::atomic<bool> Active = true;
    // All fields below belong exclusively to the polling actor.
    bool Started = false;
    bool Paused = true;
    std::map<ui64, bool> Pending;
    std::optional<ui64> DeliveredNext;
    std::optional<ui64> AcknowledgedNext;
    std::optional<ui64> CommittedNext;
};

class TKafkaReadSession final: public IMessageStreamReadSession, public RdKafka::RebalanceCb, public RdKafka::OffsetCommitCb,
                               public std::enable_shared_from_this<TKafkaReadSession> {
public:
    TKafkaReadSession(TString topic, const TKafkaMessageStreamClientSettings& config, const TMessageStreamReadSessionSettings& settings)
        : Topic(std::move(topic))
        , System(config.ActorSystem)
        , PoolId(config.ActorPoolId)
        , MemoryLimit(settings.MaxMemoryUsageBytes ? settings.MaxMemoryUsageBytes : 16ULL << 20)
    {
        settings.Validate();
        if (!settings.Consumer || !settings.PartitionIds.empty() || settings.RequireWriteTime || settings.ReadFromWriteTime || settings.Retry) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported)
                << "Kafka requires managed assignment with a consumer; explicit partitions, WriteTime and custom retries are unsupported";
        }
        if (!System) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Kafka read session requires an actor system";
        }
        auto conf = MakeConfig(config);
        Set(*conf, "group.id", std::string(*settings.Consumer));
        Set(*conf, "group.protocol", "classic");
        Set(*conf, "partition.assignment.strategy", "range");
        Set(*conf, "enable.auto.commit", "false");
        Set(*conf, "enable.auto.offset.store", "false");
        Set(*conf, "isolation.level", "read_committed");
        Set(*conf, "queued.max.messages.kbytes", std::to_string(std::max<ui64>(1, MemoryLimit / 1024)));
        Set(*conf, "auto.offset.reset",
            settings.OffsetResetPolicy == EMessageStreamOffsetResetPolicy::Earliest ? "earliest"
            : settings.OffsetResetPolicy == EMessageStreamOffsetResetPolicy::Latest ? "latest"
                                                                                    : "error");
        std::string error;
        if (conf->set("rebalance_cb", static_cast<RdKafka::RebalanceCb*>(this), error) != RdKafka::Conf::CONF_OK ||
            conf->set("offset_commit_cb", static_cast<RdKafka::OffsetCommitCb*>(this), error) != RdKafka::Conf::CONF_OK) {
            ythrow TMessageStreamException(EMessageStreamStatus::InternalError) << "Cannot configure Kafka callbacks";
        }
        Consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), error));
        if (!Consumer) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Cannot create Kafka consumer: " << error;
        }
        Check(Consumer->subscribe({ std::string(Topic) }), "subscribe");
    }

    ~TKafkaReadSession() override {
        if (ConsumerQueue) {
            rd_kafka_queue_cb_event_enable(ConsumerQueue, nullptr, nullptr);
            rd_kafka_queue_destroy(ConsumerQueue);
        }
    }

    void Start();

    NThreading::TFuture<void> WaitEvent() override {
        std::lock_guard lock(Mutex);
        return !Events.empty() || Closing || TerminalDelivered ? NThreading::MakeFuture() : Ready.GetFuture();
    }

    std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamGetEventsSettings& settings) override {
        if (settings.Block) {
            ythrow TMessageStreamException(EMessageStreamStatus::Unsupported) << "Blocking Kafka GetEvents is unsupported";
        }
        std::vector<TMessageStreamReadEvent> result;
        {
            std::lock_guard lock(Mutex);
            if (Closing || TerminalDelivered || !settings.MaxByteSize || (settings.MaxEventsCount && !*settings.MaxEventsCount)) {
                return result;
            }
            size_t bytes = 0;
            while (!Events.empty() && bytes < settings.MaxByteSize && (!settings.MaxEventsCount || result.size() < *settings.MaxEventsCount)) {
                bytes += Events.front().second;
                BufferedBytes -= Events.front().second;
                result.push_back(std::move(Events.front().first));
                Events.pop_front();
                if (std::holds_alternative<TMessageStreamSessionClosedEvent>(result.back())) {
                    TerminalDelivered = true;
                    break;
                }
            }
            if (Events.empty() && !TerminalDelivered && !result.empty()) {
                Ready = NThreading::NewPromise();
            }
        }
        Wake();
        return result;
    }

    NThreading::TFuture<void> Close() override {
        Closing = true;
        NThreading::TPromise<void> ready;
        {
            std::lock_guard lock(Mutex);
            Events.clear();
            BufferedBytes = 0;
            ready = Ready;
        }
        ready.TrySetValue();
        Wake();
        return Closed.GetFuture();
    }

    TString GetSessionId() const override {
        return Id;
    }

    void Post(std::function<void(TKafkaReadSession&)> command) {
        TActorId actor;
        {
            std::lock_guard lock(Mutex);
            actor = PollActorId;
        }
        if (actor && !Closing) {
            System->Send(actor, new TEvKafkaCommand(std::move(command)));
        }
    }

    void Initialize(const TActorId& actor) {
        {
            std::lock_guard lock(Mutex);
            PollActorId = actor;
        }
        ConsumerQueue = rd_kafka_queue_get_consumer(Consumer->c_ptr());
        rd_kafka_queue_cb_event_enable(ConsumerQueue, [](rd_kafka_t*, void* opaque) {
            // Only signal readiness: calling Kafka API in this callback is forbidden.
            static_cast<TKafkaReadSession*>(opaque)->Wake();
        }, this);
    }

    bool Poll() {
        WakePending = false;
        if (Closing) {
            return PollClose();
        }
        if (Failed) {
            return false;
        }
        try {
            UpdatePauses();
            for (size_t count = 0; count < 128 && !Failed; ++count) {
                std::unique_ptr<RdKafka::Message> message(Consumer->consume(0));
                if (message->err() == RdKafka::ERR__TIMED_OUT) {
                    break;
                }
                if (message->err() == RdKafka::ERR__PARTITION_EOF) {
                    continue;   // A temporary end is not permanent partition exhaustion.
                }
                Check(message->err(), "consume");
                const auto it = Controls.find(message->partition());
                if (it == Controls.end()) {
                    continue;   // A record from an assignment already revoked by a callback.
                }
                auto control = it->second;
                if (message->offset() < 0) {
                    ythrow TMessageStreamException(EMessageStreamStatus::InternalError) << "Negative Kafka record offset";
                }
                TMessageStreamRecord record;
                record.Id = { control->Id, static_cast<ui64>(message->offset()) };
                if (message->payload()) {
                    record.Data = TString(static_cast<const char*>(message->payload()), message->len());
                }
                if (message->key()) {
                    record.Key = TString(*message->key());
                }
                const auto timestamp = message->timestamp();
                if (timestamp.timestamp >= 0) {
                    if (timestamp.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
                        record.CreateTime = TInstant::MilliSeconds(timestamp.timestamp);
                    } else if (timestamp.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME) {
                        record.WriteTime = TInstant::MilliSeconds(timestamp.timestamp);
                    }
                }
                size_t bytes = sizeof(TMessageStreamRecord) + (record.Data ? record.Data->size() : 0);
                bytes += record.Key ? record.Key->size() : 0;
                if (auto* headers = message->headers()) {
                    for (const auto& header : headers->get_all()) {
                        TMessageStreamAttribute attribute{ .Name = TString(header.key()) };
                        if (header.value()) {
                            attribute.Value = TString(static_cast<const char*>(header.value()), header.value_size());
                        }
                        bytes += attribute.Name.size() + (attribute.Value ? attribute.Value->size() : 0);
                        record.Attributes.push_back(std::move(attribute));
                    }
                }
                control->Pending.emplace(record.Id.Offset, false);
                control->DeliveredNext = record.Id.Offset + 1;
                Enqueue(TMessageStreamDataEvent{ control, { std::move(record) } }, bytes);
                if (BufferFull()) {
                    UpdatePauses();
                    break;
                }
                if (count == 127) {
                    Wake();
                }
            }
            TryCommit();
        } catch (const TMessageStreamException& ex) {
            Fail(ex.GetStatus(), ex.what());
        } catch (const std::exception& ex) {
            Fail(EMessageStreamStatus::InternalError, ex.what());
        }
        return false;
    }

    void Acknowledge(const std::shared_ptr<TKafkaPartitionControl>& control, ui64 start, ui64 end) {
        if (!control->Active) {
            return;
        }
        for (auto it = control->Pending.lower_bound(start); it != control->Pending.end() && it->first < end; ++it) {
            it->second = true;
        }
        bool advanced = false;
        while (!control->Pending.empty() && control->Pending.begin()->second) {
            control->Pending.erase(control->Pending.begin());
            advanced = true;
        }
        if (advanced) {
            // Gaps in the Kafka log do not represent unprocessed records.
            control->AcknowledgedNext = control->Pending.empty() ? control->DeliveredNext : std::optional<ui64>(control->Pending.begin()->first);
        }
        TryCommit();
    }

    void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode error, std::vector<RdKafka::TopicPartition*>& partitions) override {
        try {
            for (auto& [_, control] : Controls) {
                control->Active = false;
                if (!Closing && !Failed) {
                    if (!consumer->assignment_lost()) {
                        Enqueue(TMessageStreamPartitionStopRequestedEvent{ control });
                    }
                    Enqueue(TMessageStreamPartitionClosedEvent{ control });
                }
            }
            Controls.clear();
            if (error == RdKafka::ERR__ASSIGN_PARTITIONS && !Closing && !Failed) {
                Check(consumer->assign(partitions), "accept group assignment");
                Check(consumer->pause(partitions), "pause new assignment");
                for (const auto* partition : partitions) {
                    auto control = std::make_shared<TKafkaPartitionControl>(weak_from_this(), partition->partition());
                    Controls.emplace(partition->partition(), control);
                    Enqueue(TMessageStreamPartitionStartRequestedEvent{ .PartitionControl = std::move(control) });
                }
            } else {
                Check(consumer->unassign(), "revoke group assignment");
            }
        } catch (const std::exception& ex) {
            Fail(EMessageStreamStatus::External, ex.what());
        }
    }

    void offset_commit_cb(RdKafka::ErrorCode error, std::vector<RdKafka::TopicPartition*>& offsets) override {
        // Only one explicit commit is in flight. Preserve its assignment identities
        // across rebalance; an old completion must never acknowledge a new control.
        auto committed = std::move(CommitInFlight);
        CommitInFlight.clear();
        if (error != RdKafka::ERR_NO_ERROR) {
            for (const auto& [_, request] : committed) {
                if (request.first->Active) {
                    Fail(ConvertError(error), TString(RdKafka::err2str(error)));
                    break;
                }
            }
            return;
        }
        for (const auto* offset : offsets) {
            const auto it = committed.find(offset->partition());
            if (it == committed.end() || !it->second.first->Active) {
                continue;
            }
            const auto status = offset->err();
            if (status != RdKafka::ERR_NO_ERROR) {
                Fail(ConvertError(status), TString(RdKafka::err2str(status)));
                return;
            }
            it->second.first->CommittedNext = it->second.second;
        }
    }

    std::atomic<bool> Closing = false;

private:
    friend class TKafkaPartitionControl;
    friend class TKafkaPollActor;

    void Wake() {
        TActorId actor;
        {
            std::lock_guard lock(Mutex);
            actor = PollActorId;
        }
        if (actor && !WakePending.exchange(true)) {
            System->Send(actor, new TEvKafkaPoll());
        }
    }

    bool BufferFull() {
        std::lock_guard lock(Mutex);
        return BufferedBytes >= MemoryLimit;
    }

    void Enqueue(TMessageStreamReadEvent event, size_t bytes = 0) {
        NThreading::TPromise<void> ready;
        {
            std::lock_guard lock(Mutex);
            if (Closing || (Failed && !std::holds_alternative<TMessageStreamSessionClosedEvent>(event))) {
                return;
            }
            Events.emplace_back(std::move(event), bytes);
            BufferedBytes += bytes;
            ready = Ready;
        }
        ready.TrySetValue();
    }

    void Fail(EMessageStreamStatus status, const TString& error) {
        if (!Failed && !Closing) {
            Failed = true;
            Enqueue(TMessageStreamSessionClosedEvent{ status, NYql::TIssues{ NYql::TIssue(error) } });
        }
    }

    void UpdatePauses() {
        const bool full = BufferFull();
        for (auto& [id, control] : Controls) {
            const bool pause = full || !control->Started;
            if (pause != control->Paused) {
                std::unique_ptr<RdKafka::TopicPartition> partition(RdKafka::TopicPartition::create(std::string(Topic), id));
                std::vector<RdKafka::TopicPartition*> partitions{ partition.get() };
                Check(pause ? Consumer->pause(partitions) : Consumer->resume(partitions), "update backpressure");
                control->Paused = pause;
            }
        }
    }

    void TryCommit() {
        if (!CommitInFlight.empty() || Failed || Closing) {
            return;
        }
        std::vector<std::unique_ptr<RdKafka::TopicPartition>> owned;
        std::vector<RdKafka::TopicPartition*> offsets;
        for (const auto& [id, control] : Controls) {
            if (control->AcknowledgedNext && (!control->CommittedNext || *control->AcknowledgedNext > *control->CommittedNext)) {
                owned.emplace_back(RdKafka::TopicPartition::create(std::string(Topic), id, *control->AcknowledgedNext));
                offsets.push_back(owned.back().get());
                CommitInFlight.emplace(id, std::make_pair(control, *control->AcknowledgedNext));
            }
        }
        if (!offsets.empty()) {
            Check(Consumer->commitAsync(offsets), "commit acknowledged offsets");
        }
    }

    bool PollClose() {
        if (!CloseQueue) {
            for (auto& [_, control] : Controls) {
                control->Active = false;
            }
            Controls.clear();
            rd_kafka_queue_cb_event_enable(ConsumerQueue, nullptr, nullptr);
            rd_kafka_queue_destroy(ConsumerQueue);
            ConsumerQueue = nullptr;
            CloseQueue.reset(RdKafka::Queue::create(Consumer.get()));
            std::unique_ptr<RdKafka::Error> error(Consumer->close(CloseQueue.get()));
            if (error) {
                Closed.TrySetException(std::make_exception_ptr(yexception() << "Kafka close: " << error->str()));
                return true;
            }
        }
        CloseQueue->poll(0);
        if (!Consumer->closed()) {
            return false;
        }
        CloseQueue.reset();
        Consumer.reset();
        Closed.TrySetValue();
        return true;
    }

    const TString Topic;
    const TString Id = CreateGuidAsString();
    TActorSystem* const System;
    const ui32 PoolId;
    const ui64 MemoryLimit;
    std::mutex Mutex;
    TActorId PollActorId;
    std::atomic<bool> WakePending = false;
    NThreading::TPromise<void> Ready = NThreading::NewPromise();
    NThreading::TPromise<void> Closed = NThreading::NewPromise();
    std::deque<std::pair<TMessageStreamReadEvent, size_t>> Events;
    size_t BufferedBytes = 0;
    bool TerminalDelivered = false;
    bool Failed = false;
    std::unique_ptr<RdKafka::KafkaConsumer> Consumer;
    rd_kafka_queue_t* ConsumerQueue = nullptr;
    std::unique_ptr<RdKafka::Queue> CloseQueue;
    std::map<int32_t, std::shared_ptr<TKafkaPartitionControl>> Controls;
    std::map<int32_t, std::pair<std::shared_ptr<TKafkaPartitionControl>, ui64>> CommitInFlight;
};

class TKafkaPollActor final: public TActorBootstrapped<TKafkaPollActor> {
public:
    explicit TKafkaPollActor(std::shared_ptr<TKafkaReadSession> session)
        : Session(std::move(session))
    {
    }

    void Bootstrap() {
        Become(&TKafkaPollActor::StateWork);
        Session->Initialize(SelfId());
        Pump();
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateWork, {
                cFunc(TEvKafkaPoll::EventType, Pump);
                hFunc(TEvKafkaCommand, Handle);
                cFunc(TEvents::TEvWakeup::EventType, OnTimer);
            });

private:
    void Pump() {
        if (Session->Poll()) {
            PassAway();
        }
    }

    void Handle(TEvKafkaCommand::TPtr& event) {
        if (!Session->Closing) {
            try {
                event->Get()->Command(*Session);
            } catch (const std::exception& ex) {
                Session->Fail(EMessageStreamStatus::External, ex.what());
            }
        }
        Pump();
    }

    void OnTimer() {
        Pump();
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    std::shared_ptr<TKafkaReadSession> Session;
};

void TKafkaReadSession::Start() {
    System->Register(new TKafkaPollActor(shared_from_this()), TMailboxType::HTSwap, PoolId);
}

void TKafkaPartitionControl::ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) {
    if (!Active) {
        return;
    }
    if (startOffset || maxOffset) {
        ythrow TMessageStreamException(EMessageStreamStatus::Unsupported) << "Explicit Kafka start/end offsets are unsupported";
    }
    if (auto session = Session.lock()) {
        session->Post([self = shared_from_this()](auto&) {
            if (self->Active) {
                self->Started = true;
            }
        });
    }
}

void TKafkaPartitionControl::RequestStatus() {
    if (auto session = Session.lock(); session && Active) {
        // Bounds are deliberately absent: a high watermark is not a read_committed LSO.
        session->Post([self = shared_from_this()](auto& owner) {
            if (self->Active) {
                owner.Enqueue(TMessageStreamPartitionStatusEvent{
                    .PartitionControl = self, .CommittedOffset = self->CommittedNext, .ReadOffset = self->DeliveredNext });
            }
        });
    }
}

bool TKafkaPartitionControl::AcknowledgeRange(ui64 start, ui64 end) {
    if (start >= end) {
        ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Invalid Kafka acknowledgement range";
    }
    if (auto session = Session.lock(); session && Active && !session->Closing) {
        session->Post([self = shared_from_this(), start, end](auto& owner) {
            owner.Acknowledge(self, start, end);
        });
        return true;
    }
    return false;
}

class TKafkaMessageStreamClient final: public IMessageStreamClient {
public:
    TKafkaMessageStreamClient(TString topic, TKafkaMessageStreamClientSettings settings)
        : Topic(std::move(topic))
        , Settings(std::move(settings))
    {
        if (Topic.empty() || Settings.BootstrapBrokers.empty()) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Kafka topic and bootstrap brokers must be nonempty";
        }
    }

    const TString& GetStream() const override {
        return Topic;
    }

    std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSessionSettings& settings) override {
        auto session = std::make_shared<TKafkaReadSession>(Topic, Settings, settings);
        session->Start();
        return session;
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream() override {
        auto promise = NThreading::NewPromise<TMessageStreamResult<TMessageStreamDescription>>();
        // Metadata C++ API is blocking; do not run it in a compiler/reader actor.
        std::thread([topic = Topic, settings = Settings, promise]() mutable {
            try {
                auto conf = MakeConfig(settings);
                std::string error;
                std::unique_ptr<RdKafka::Producer> client(RdKafka::Producer::create(conf.get(), error));
                if (!client) {
                    ythrow yexception() << "Cannot create Kafka metadata client: " << error;
                }
                std::unique_ptr<RdKafka::Topic> handle(RdKafka::Topic::create(client.get(), std::string(topic), nullptr, error));
                if (!handle) {
                    ythrow yexception() << "Cannot create Kafka topic handle: " << error;
                }
                RdKafka::Metadata* raw = nullptr;
                Check(client->metadata(false, handle.get(), &raw, 10000), "describe topic");
                std::unique_ptr<RdKafka::Metadata> metadata(raw);
                TMessageStreamDescription description;
                bool found = false;
                for (const auto* entry : *metadata->topics()) {
                    if (entry->topic() == std::string(topic)) {
                        Check(entry->err(), "describe topic");
                        found = true;
                        for (const auto* partition : *entry->partitions()) {
                            Check(partition->err(), "describe partition");
                            if (partition->id() < 0) {
                                ythrow TMessageStreamException(EMessageStreamStatus::InternalError) << "Negative Kafka partition ID";
                            }
                            description.Partitions.push_back({ .PartitionId = { static_cast<ui64>(partition->id()) } });
                        }
                    }
                }
                if (!found || description.Partitions.empty()) {
                    ythrow TMessageStreamException(EMessageStreamStatus::NotFound) << "Kafka topic has no readable partitions";
                }
                promise.SetValue(TMessageStreamResult<TMessageStreamDescription>::Success(std::move(description)));
            } catch (const TMessageStreamException& ex) {
                promise.SetValue(TMessageStreamResult<TMessageStreamDescription>::Failure(ex.GetStatus(), { NYql::TIssue(ex.what()) }));
            } catch (...) {
                promise.SetException(std::current_exception());
            }
        }).detach();
        return promise.GetFuture();
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString&, const TMessageStreamDescribeConsumerSettings&) override {
        return NThreading::MakeFuture(TMessageStreamResult<TMessageStreamConsumerDescription>::Failure(EMessageStreamStatus::Unsupported));
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(TMessageStreamPartitionId id) override {
        return DescribeStream().Apply([id](const auto& future) {
            const auto& description = future.GetValue();
            if (!description.IsSuccess()) {
                return TMessageStreamResult<TMessageStreamPartitionDescription>::Failure(description.Status, description.Issues);
            }
            for (const auto& partition : description.Value.Partitions) {
                if (partition.PartitionId == id) {
                    return TMessageStreamResult<TMessageStreamPartitionDescription>::Success({ .PartitionId = id });
                }
            }
            return TMessageStreamResult<TMessageStreamPartitionDescription>::Failure(EMessageStreamStatus::NotFound);
        });
    }

    NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(
        TMessageStreamPartitionId, const TString&, ui64) override {
        return NThreading::MakeFuture(TMessageStreamResult<TMessageStreamConsumerPosition>::Failure(EMessageStreamStatus::Unsupported));
    }

private:
    const TString Topic;
    const TKafkaMessageStreamClientSettings Settings;
};

}   // namespace

std::shared_ptr<IMessageStreamClient> CreateKafkaMessageStreamClient(TString topic, TKafkaMessageStreamClientSettings settings) {
    return std::make_shared<TKafkaMessageStreamClient>(std::move(topic), std::move(settings));
}

}   // namespace NFq
