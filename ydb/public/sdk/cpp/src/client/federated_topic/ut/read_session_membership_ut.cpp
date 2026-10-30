#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/retry_policy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <ydb/public/sdk/cpp/src/client/federated_topic/ut/fds_mock/fds_mock.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace NYdb::NFederatedTopic::NTests {

namespace {

using TDbStatus = Ydb::FederationDiscovery::DatabaseInfo::Status;

struct TDbSpec {
    std::string Name;
    TDbStatus Status = TDbStatus::DatabaseInfo_Status_AVAILABLE;
    std::string Path = "/Root";
};

// Runs posted callbacks only when the test asks. Used to prove that a terminal
// close notification is queued to the handlers executor instead of running on
// the discovery thread.
class TManualExecutor final : public IExecutor {
public:
    void Post(TFunction&& function) override {
        with_lock (Lock) {
            Pending.push_back(std::move(function));
        }
    }

    bool IsAsync() const override {
        return true;
    }

    void Stop() override {
    }

    size_t Size() const {
        with_lock (Lock) {
            return Pending.size();
        }
    }

    void Drain() {
        std::vector<TFunction> batch;
        with_lock (Lock) {
            batch.swap(Pending);
        }
        for (auto& function : batch) {
            function();
        }
    }

protected:
    void DoStart() override {
    }

private:
    mutable TAdaptiveLock Lock;
    std::vector<TFunction> Pending;
};

struct TAssignment {
    std::string Database;
    std::string Topic;
};

class TMembershipFixture {
public:
    TMembershipFixture(const char* name, size_t partitions,
                       NTopic::IRetryPolicy::TPtr discoveryRetryPolicy = NTopic::IRetryPolicy::GetNoRetryPolicy())
        : Partitions(partitions)
        , Setup(std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
              TString(name), false, ::NPersQueue::TTestServer::LOGGED_SERVICES,
              NActors::NLog::PRI_ERROR, 1, partitions))
    {
        Setup->Start(true, true);
        Fds.Port = Setup->GetGrpcPort();
        const ui16 servicePort = Setup->GetPortManager()->GetPort(4285);
        Server = Setup->StartGrpcService(servicePort, &Fds);
        Databases.push_back(TDbSpec{"dc1"});

        Responder = std::thread([this] {
            while (!Stop.load()) {
                auto request = Fds.GetNextPendingRequest();
                if (!request) {
                    Sleep(TDuration::MilliSeconds(10));
                    continue;
                }
                request->Result.SetValue(Compose());
            }
            const auto deadline = TInstant::Now() + TDuration::Seconds(1);
            do {
                if (auto request = Fds.GetNextPendingRequest()) {
                    request->Result.SetValue(Compose());
                } else {
                    Sleep(TDuration::MilliSeconds(10));
                }
            } while (TInstant::Now() < deadline);
        });

        TDriverConfig config;
        config.SetEndpoint(TStringBuilder() << "localhost:" << servicePort);
        config.SetDatabase("/Root");
        config.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_ERR).Release()));
        Driver = std::make_unique<TDriver>(config);

        TFederatedTopicClientSettings clientSettings;
        clientSettings.FederationDiscoveryInterval(TDuration::MilliSeconds(200));
        clientSettings.RetryPolicy(std::move(discoveryRetryPolicy));
        Client = std::make_unique<TFederatedTopicClient>(*Driver, clientSettings);
    }

    ~TMembershipFixture() {
        Client.reset();
        Driver.reset();
        Stop.store(true);
        if (Responder.joinable()) {
            Responder.join();
        }
    }

    void SetDatabases(std::vector<TDbSpec> databases) {
        with_lock (Lock) {
            Databases = std::move(databases);
        }
    }

    void FailNextDiscoveries() {
        FailDiscoveries(-1);
    }

    // count < 0 fails every following discovery, count == 0 stops failing.
    void FailDiscoveries(int count, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::UNAVAILABLE) {
        FailureStatus.store(status);
        FailuresLeft.store(count);
    }

    int PendingFailures() const {
        return FailuresLeft.load();
    }

    // Pumps the session until a data event carrying `text` arrives and hands it to the caller.
    std::optional<TReadSessionEvent::TDataReceivedEvent> WaitForData(IFederatedReadSession& session, const std::string& text) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline) {
            if (!session.WaitEvent().Wait(TDuration::MilliSeconds(200))) {
                continue;
            }
            while (auto event = session.GetEvent(false)) {
                if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    for (const auto& message : data->GetMessages()) {
                        if (message.GetData() == text) {
                            return std::move(*data);
                        }
                    }
                    continue;
                }
                if (Handle(*event)) {
                    return std::nullopt;
                }
            }
        }
        return std::nullopt;
    }

    int FailedResponses() const {
        return Failed.load();
    }

    int SucceededResponses() const {
        return Succeeded.load();
    }

    std::shared_ptr<IFederatedReadSession> StartReader() {
        return StartReader({});
    }

    std::shared_ptr<IFederatedReadSession> StartReader(TFederatedReadSessionSettings settings) {
        if (settings.Topics_.empty()) {
            settings.AppendTopics(Setup->GetTestTopic());
        }
        if (settings.ConsumerName_.empty()) {
            settings.ConsumerName("shared/user");
        }
        settings.MaxMemoryUsageBytes(1_MB);
        return Client->CreateReadSession(settings);
    }

    bool Pump(IFederatedReadSession& session) {
        // A closed session returns its close event on every call.
        while (auto event = session.GetEvent(false)) {
            if (Handle(*event)) {
                return true;
            }
        }
        return false;
    }

    // True once the session has gone quiet: a fresh WaitEvent stays blocked for a whole
    // discovery interval. Used before asserting that something stopped being read, where a
    // fixed sleep would race the refresh.
    bool WaitUntilIdle(IFederatedReadSession& session) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline) {
            if (!session.WaitEvent().Wait(TDuration::Seconds(1))) {
                return true;
            }
            Pump(session);
        }
        return false;
    }

    bool WaitUntil(IFederatedReadSession& session, const std::function<bool()>& predicate) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline) {
            if (predicate()) {
                return true;
            }
            if (!session.WaitEvent().Wait(TDuration::MilliSeconds(200))) {
                continue;
            }
            Pump(session);
        }
        return predicate();
    }

    bool AllOn(const std::string& database) const {
        if (Assignments.size() != Partitions) {
            return false;
        }
        for (const auto& [partition, assignment] : Assignments) {
            Y_UNUSED(partition);
            if (assignment.Database != database) {
                return false;
            }
        }
        return true;
    }

    bool HasDatabase(const std::string& database) const {
        for (const auto& [partition, assignment] : Assignments) {
            Y_UNUSED(partition);
            if (assignment.Database == database) {
                return true;
            }
        }
        return false;
    }

    bool HasTopicSuffix(const std::string& suffix) const {
        for (const auto& [partition, assignment] : Assignments) {
            Y_UNUSED(partition);
            if (assignment.Topic.find(suffix) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    void Write(const TString& text) {
        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(Setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        auto writer = Setup->GetPersQueueClient().CreateSimpleBlockingWriteSession(writeSettings);
        UNIT_ASSERT(writer->Write(text));
        writer->Close();
    }

    bool Saw(const std::string& text) const {
        return std::find(Messages.begin(), Messages.end(), text) != Messages.end();
    }

    // A retired subsession is not polled anymore, so its stop events do not clear Assignments.
    // Silence on the topic is the observable sign that reading has stopped.
    bool StaysSilent(IFederatedReadSession& session, const std::string& text, TDuration duration) {
        const auto deadline = TInstant::Now() + duration;
        while (TInstant::Now() < deadline) {
            Pump(session);
            if (Saw(text)) {
                return false;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        return !Saw(text);
    }

    const size_t Partitions;
    std::unordered_map<std::string, int> OriginStarts;
    // Partition sessions closed with ConnectionLost, by read source database.
    std::unordered_map<std::string, int> Lost;
    std::shared_ptr<NPersQueue::NTests::TPersQueueYdbSdkTestSetup> Setup;
    std::unordered_map<ui64, TAssignment> Assignments;
    std::vector<std::string> Messages;
    int SessionClosedEvents = 0;
    EStatus LastSessionStatus = EStatus::STATUS_UNDEFINED;

private:
    bool Handle(TReadSessionEvent::TEvent& event) {
        if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
            const auto session = start->GetFederatedPartitionSession();
            Assignments[session->GetPartitionId()] = TAssignment{
                session->GetReadSourceDatabaseName(),
                session->GetTopicPath(),
            };
            ++OriginStarts[session->GetTopicOriginDatabaseName()];
            start->Confirm();
            return false;
        }
        if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
            for (const auto& message : data->GetMessages()) {
                Messages.emplace_back(message.GetData());
            }
            return false;
        }
        if (auto* stop = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
            stop->Confirm();
            return false;
        }
        if (auto* end = std::get_if<TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
            end->Confirm();
            return false;
        }
        if (auto* closed = std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
            const auto session = closed->GetFederatedPartitionSession();
            const std::string database = session->GetReadSourceDatabaseName();
            if (closed->GetReason() == NTopic::TReadSessionEvent::TPartitionSessionClosedEvent::EReason::ConnectionLost) {
                ++Lost[database];
            }
            // Another database may already have taken the partition over.
            auto it = Assignments.find(session->GetPartitionId());
            if (it != Assignments.end() && it->second.Database == database) {
                Assignments.erase(it);
            }
            return false;
        }
        if (auto* sessionClosed = std::get_if<TSessionClosedEvent>(&event)) {
            ++SessionClosedEvents;
            LastSessionStatus = sessionClosed->GetStatus();
            return true;
        }
        return false;
    }

    TFederationDiscoveryServiceMock::TGrpcResult Compose() {
        int left = FailuresLeft.load();
        while (left != 0 && !FailuresLeft.compare_exchange_weak(left, left > 0 ? left - 1 : left)) {
        }
        if (left != 0) {
            ++Failed;
            Ydb::FederationDiscovery::ListFederationDatabasesResponse response;
            auto* op = response.mutable_operation();
            op->set_status(FailureStatus.load());
            op->set_ready(true);
            op->set_id("federation-membership");
            return {std::move(response), grpc::Status::OK};
        }
        ++Succeeded;

        std::vector<TDbSpec> databases;
        with_lock (Lock) {
            databases = Databases;
        }

        Ydb::FederationDiscovery::ListFederationDatabasesResponse response;
        auto* op = response.mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->set_ready(true);
        op->set_id("federation-membership");

        Ydb::FederationDiscovery::ListFederationDatabasesResult result;
        result.set_control_plane_endpoint("cp.logbroker-federation:2135");
        result.set_self_location("fancy_datacenter");
        for (const auto& database : databases) {
            auto* info = result.add_federation_databases();
            info->set_name(database.Name);
            info->set_path(database.Path);
            info->set_id(std::string("account-") + database.Name);
            info->set_endpoint("localhost:" + ToString(Fds.Port));
            info->set_location(database.Name);
            info->set_status(database.Status);
            info->set_weight(1000);
        }
        op->mutable_result()->PackFrom(result);
        return {std::move(response), grpc::Status::OK};
    }

    TFederationDiscoveryServiceMock Fds;
    std::unique_ptr<grpc::Server> Server;
    std::atomic<bool> Stop{false};
    std::atomic<int> FailuresLeft{0};
    std::atomic<Ydb::StatusIds::StatusCode> FailureStatus{Ydb::StatusIds::UNAVAILABLE};
    std::atomic<int> Failed{0};
    std::atomic<int> Succeeded{0};
    TAdaptiveLock Lock;
    std::vector<TDbSpec> Databases;
    std::thread Responder;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<TFederatedTopicClient> Client;
};

} // namespace

Y_UNIT_TEST_SUITE(ReadSessionMembership) {

    Y_UNIT_TEST(AddsDatabaseWithoutDroppingTheCurrentOne) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        auto session = fixture.StartReader();
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "dc1 should receive every partition before another database appears");

        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] {
            return fixture.Assignments.size() == fixture.Partitions && fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2");
        }), "both databases should be reading after dc2 joins");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(RemovesOneDatabaseAndKeepsTheOther) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        auto session = fixture.StartReader();
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] {
            return fixture.Assignments.size() == fixture.Partitions && fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2");
        }), "both databases should receive partitions");

        fixture.SetDatabases({TDbSpec{"dc1"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "dc2 should stop being read while dc1 keeps the partitions");
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.Lost["dc2"] >= 1; }),
            "partition sessions of the database that left should be closed as lost");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(LeavingDatabaseReportsLostPartitionsToHandler) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});

        TAdaptiveLock lock;
        std::unordered_map<std::string, int> lostByHandler;
        TFederatedReadSessionSettings settings;
        settings.FederatedEventHandlers_.PartitionSessionClosedHandler([&](TReadSessionEvent::TPartitionSessionClosedEvent& event) {
            if (event.GetReason() == NTopic::TReadSessionEvent::TPartitionSessionClosedEvent::EReason::ConnectionLost) {
                with_lock (lock) {
                    ++lostByHandler[event.GetFederatedPartitionSession()->GetReadSourceDatabaseName()];
                }
            }
        });
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] {
            return fixture.Assignments.size() == fixture.Partitions && fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2");
        }));

        fixture.SetDatabases({TDbSpec{"dc1"}});
        auto lostOn = [&](const std::string& database) {
            with_lock (lock) {
                return lostByHandler[database];
            }
        };
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return lostOn("dc2") >= 1; }),
            "PartitionSessionClosedHandler should get the partitions of the database that left");
        UNIT_ASSERT_VALUES_EQUAL(lostOn("dc1"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Lost["dc2"], 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(EmptyMembershipPausesThenResumes) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        auto session = fixture.StartReader();
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "dc1 should receive every partition");

        fixture.Write("while-up");
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.Saw("while-up"); }),
            "a message written while dc1 is readable should be delivered");

        fixture.SetDatabases({});
        UNIT_ASSERT_C(fixture.WaitUntilIdle(*session),
            "the session should go idle once the federation is empty");
        fixture.Write("while-down");
        UNIT_ASSERT_C(fixture.StaysSilent(*session, "while-down", TDuration::Seconds(3)),
            "an empty federation should stop reading without closing the session");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);

        auto first = session->WaitEvent();
        auto second = session->WaitEvent();
        UNIT_ASSERT_C(!first.Wait(TDuration::MilliSeconds(400)), "WaitEvent should block while no database is readable");

        fixture.SetDatabases({TDbSpec{"dc1"}});
        UNIT_ASSERT_C(first.Wait(TDuration::Seconds(20)) && second.Wait(TDuration::Seconds(20)),
            "both blocked WaitEvent calls should wake up when a database returns");
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "reading should resume from dc1");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(UnavailableDatabaseKeepsItsSubsession) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        auto session = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));
        const int startsBefore = fixture.OriginStarts["dc1"];

        fixture.SetDatabases({TDbSpec{"dc1", TDbStatus::DatabaseInfo_Status_UNAVAILABLE}});
        const int responses = fixture.SucceededResponses();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.SucceededResponses() >= responses + 3; }));
        fixture.Write("while-unavailable");
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.Saw("while-unavailable"); }),
            "a listed database that turns UNAVAILABLE should keep its subsession");
        UNIT_ASSERT_C(fixture.AllOn("dc1"), "partitions should stay where they were");
        UNIT_ASSERT_VALUES_EQUAL(fixture.OriginStarts["dc1"], startsBefore);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Lost["dc1"], 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(UnavailableDatabaseIsOpenedOnceAvailable) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2", TDbStatus::DatabaseInfo_Status_UNAVAILABLE}});
        auto session = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        const int responses = fixture.SucceededResponses();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.SucceededResponses() >= responses + 3; }));
        UNIT_ASSERT_C(fixture.AllOn("dc1") && !fixture.HasDatabase("dc2"),
            "a database that is UNAVAILABLE from the start should not be opened");

        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] {
            return fixture.Assignments.size() == fixture.Partitions && fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2");
        }), "dc2 should be opened once it is AVAILABLE");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(ReadOnlyDatabaseStaysReadable) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        auto session = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        fixture.SetDatabases({TDbSpec{"dc1", TDbStatus::DatabaseInfo_Status_READ_ONLY}});
        const int responses = fixture.SucceededResponses();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.SucceededResponses() >= responses + 3; }));
        UNIT_ASSERT_C(fixture.AllOn("dc1"), "READ_ONLY is still a readable federation database");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(ReadOriginalIgnoresDatabasesOutsideTheRequestedList) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});

        TFederatedReadSessionSettings settings;
        TFederatedReadSessionSettings::TReadOriginalSettings original;
        original.AddDatabase("dc1");
        settings.ReadOriginal(std::move(original));
        auto session = fixture.StartReader(std::move(settings));

        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "ReadOriginal(dc1) should not read dc2");
        UNIT_ASSERT(!fixture.HasDatabase("dc2"));

        fixture.Write("from-dc1");
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.Saw("from-dc1"); }));

        fixture.SetDatabases({TDbSpec{"dc2"}});
        UNIT_ASSERT_C(fixture.WaitUntilIdle(*session),
            "the session should go idle once the only requested database leaves");
        fixture.Write("while-dc1-is-gone");
        UNIT_ASSERT_C(fixture.StaysSilent(*session, "while-dc1-is-gone", TDuration::Seconds(3)),
            "dc2 is not requested, so removing dc1 should leave nothing to read");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);

        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.Saw("while-dc1-is-gone") && fixture.AllOn("dc1"); }),
            "dc1 should be picked up again and dc2 should still be ignored");
    }

    Y_UNIT_TEST(FirstSnapshotWithoutRequestedDatabaseClosesSession) {
        TMembershipFixture fixture(__FUNCTION__, 1);
        auto executor = std::make_shared<TManualExecutor>();
        EStatus handlerStatus = EStatus::STATUS_UNDEFINED;

        TFederatedReadSessionSettings settings;
        TFederatedReadSessionSettings::TReadOriginalSettings original;
        original.AddDatabase("missing");
        settings.ReadOriginal(std::move(original));
        settings.EventHandlers_.HandlersExecutor(executor);
        settings.FederatedEventHandlers_.SessionClosedHandler([&](const TSessionClosedEvent& event) {
            handlerStatus = event.GetStatus();
        });
        auto session = fixture.StartReader(std::move(settings));

        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline && executor->Size() == 0) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(executor->Size() >= 1, "a first snapshot with no eligible database should close the session");
        executor->Drain();
        UNIT_ASSERT_VALUES_EQUAL(handlerStatus, EStatus::BAD_REQUEST);
        session->Close(TDuration::Zero());
    }

    Y_UNIT_TEST(BadRequestOfOneReaderDoesNotStopAnother) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        auto healthy = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*healthy, [&] { return fixture.AllOn("dc1"); }));

        TFederatedReadSessionSettings settings;
        TFederatedReadSessionSettings::TReadOriginalSettings original;
        original.AddDatabase("missing");
        settings.ReadOriginal(std::move(original));
        auto failing = fixture.StartReader(std::move(settings));

        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        std::optional<TReadSessionEvent::TEvent> closed;
        while (TInstant::Now() < deadline && !closed) {
            if (!failing->WaitEvent().Wait(TDuration::MilliSeconds(200))) {
                continue;
            }
            while (auto event = failing->GetEvent(false)) {
                if (std::holds_alternative<TSessionClosedEvent>(*event)) {
                    closed = std::move(event);
                    break;
                }
            }
        }
        UNIT_ASSERT(closed);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TSessionClosedEvent>(*closed).GetStatus(), EStatus::BAD_REQUEST);

        const int responses = fixture.SucceededResponses();
        UNIT_ASSERT(fixture.WaitUntil(*healthy, [&] { return fixture.SucceededResponses() >= responses + 3; }));
        UNIT_ASSERT_C(fixture.AllOn("dc1"), "the healthy reader must keep the observer snapshot without BAD_REQUEST");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(LeavingDatabaseDoesNotNotifySessionClosed) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        std::atomic<int> handlerCalls{0};
        TFederatedReadSessionSettings settings;
        settings.FederatedEventHandlers_.SessionClosedHandler([&](const TSessionClosedEvent&) {
            ++handlerCalls;
        });
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        fixture.SetDatabases({TDbSpec{"dc2"}});
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc2"); }));
        UNIT_ASSERT_VALUES_EQUAL(handlerCalls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(ExplicitCloseStillNotifiesHandler) {
        TMembershipFixture fixture(__FUNCTION__, 1);
        std::atomic<int> handlerCalls{0};
        TFederatedReadSessionSettings settings;
        settings.FederatedEventHandlers_.SessionClosedHandler([&](const TSessionClosedEvent&) {
            ++handlerCalls;
        });
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        UNIT_ASSERT(session->Close(TDuration::Seconds(5)));
        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (TInstant::Now() < deadline && handlerCalls.load() == 0) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(handlerCalls.load() >= 1, "explicit Close should still deliver SessionClosedHandler");
    }

    Y_UNIT_TEST(ExplicitCloseKeepsQueuedHandlers) {
        TMembershipFixture fixture(__FUNCTION__, 1);
        auto executor = std::make_shared<TManualExecutor>();
        std::atomic<int> acks{0};
        TFederatedReadSessionSettings settings;
        settings.EventHandlers_.HandlersExecutor(executor);
        settings.FederatedEventHandlers_.CommitOffsetAcknowledgementHandler([&](TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) {
            ++acks;
        });
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        fixture.Write("to-commit");
        auto data = fixture.WaitForData(*session, "to-commit");
        UNIT_ASSERT(data);
        data->Commit();
        const auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline && executor->Size() == 0) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(executor->Size() >= 1, "the commit acknowledgement handler should be queued");

        session->Close(TDuration::Zero());
        executor->Drain();
        UNIT_ASSERT_VALUES_EQUAL_C(acks.load(), 1, "a handler queued before Close should still get its event");
    }

    Y_UNIT_TEST(BadRequestAfterSuccessKeepsMembership) {
        TMembershipFixture fixture(__FUNCTION__, 2);
        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        auto session = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] {
            return fixture.Assignments.size() == fixture.Partitions && fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2");
        }));

        const int failed = fixture.FailedResponses();
        fixture.FailDiscoveries(-1, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.FailedResponses() >= failed + 3; }),
            "discovery should keep polling after BAD_REQUEST");
        UNIT_ASSERT_C(fixture.HasDatabase("dc1") && fixture.HasDatabase("dc2"),
            "BAD_REQUEST after a federated answer must not replace the membership");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Lost["dc1"], 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Lost["dc2"], 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);

        fixture.SetDatabases({TDbSpec{"dc1"}});
        fixture.FailDiscoveries(0);
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1") && fixture.Lost["dc2"] >= 1; }),
            "membership updates should resume once discovery answers again");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(DiscoveryRetryBudgetIsRestoredAfterSuccess) {
        // One retry per outage. Without a reset the second outage would find the budget spent.
        TMembershipFixture fixture(__FUNCTION__, 1,
            NTopic::IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(50), TDuration::MilliSeconds(50), 1));
        auto session = fixture.StartReader();
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        for (int outage = 0; outage < 3; ++outage) {
            fixture.FailDiscoveries(1);
            UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.PendingFailures() == 0; }));
            const int succeeded = fixture.SucceededResponses();
            UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.SucceededResponses() > succeeded; }),
                "discovery should recover after outage " << outage);
            UNIT_ASSERT_VALUES_EQUAL_C(fixture.SessionClosedEvents, 0, "outage " << outage);
        }

        fixture.Write("after-outages");
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.Saw("after-outages"); }));
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

    Y_UNIT_TEST(DiscoveryFailureNotifiesThroughHandlersExecutor) {
        TMembershipFixture fixture(__FUNCTION__, 1);
        auto executor = std::make_shared<TManualExecutor>();
        std::atomic<int> handlerCalls{0};
        EStatus handlerStatus = EStatus::STATUS_UNDEFINED;

        TFederatedReadSessionSettings settings;
        settings.EventHandlers_.HandlersExecutor(executor);
        settings.FederatedEventHandlers_.SessionClosedHandler([&](const TSessionClosedEvent& event) {
            ++handlerCalls;
            handlerStatus = event.GetStatus();
        });
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }));

        fixture.FailNextDiscoveries();
        auto deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline && fixture.SessionClosedEvents == 0) {
            fixture.Pump(*session);
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(fixture.SessionClosedEvents >= 1, "a failed discovery should close the session");
        // The executor runs nothing until drained, so the discovery thread did not call the handler.
        UNIT_ASSERT_VALUES_EQUAL(handlerCalls.load(), 0);

        deadline = TInstant::Now() + TDuration::Seconds(20);
        while (TInstant::Now() < deadline && handlerCalls.load() == 0) {
            executor->Drain();
            Sleep(TDuration::MilliSeconds(50));
        }
        executor->Drain();
        UNIT_ASSERT_VALUES_EQUAL(handlerCalls.load(), 1);
        UNIT_ASSERT(handlerStatus != EStatus::SUCCESS);
        UNIT_ASSERT(handlerStatus != EStatus::STATUS_UNDEFINED);
    }

    Y_UNIT_TEST(ReadMirroredFollowsAddedAndRemovedDatabase) {
        TMembershipFixture fixture(__FUNCTION__, 1);
        fixture.Setup->CreateTopic(fixture.Setup->GetTestTopic() + "-mirrored-from-dc2", fixture.Setup->GetLocalCluster());

        TFederatedReadSessionSettings settings;
        settings.ReadMirrored("dc1");
        auto session = fixture.StartReader(std::move(settings));
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.AllOn("dc1"); }),
            "the mirrored reader should start from dc1");

        fixture.SetDatabases({TDbSpec{"dc1"}, TDbSpec{"dc2"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.OriginStarts["dc2"] > 0; }),
            "a database that joins should add its mirrored topic");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);

        const int startsBeforeRemoval = fixture.OriginStarts["dc1"];
        fixture.SetDatabases({TDbSpec{"dc1"}});
        UNIT_ASSERT_C(fixture.WaitUntil(*session, [&] { return fixture.OriginStarts["dc1"] > startsBeforeRemoval; }),
            "removing dc2 should rebuild the mirrored reader and keep dc1");
        UNIT_ASSERT_VALUES_EQUAL(fixture.SessionClosedEvents, 0);
    }

}

} // namespace NYdb::NFederatedTopic::NTests
