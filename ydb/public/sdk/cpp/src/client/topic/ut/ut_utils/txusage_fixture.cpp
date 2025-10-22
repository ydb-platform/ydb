#include "txusage_fixture.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pqtablet/cache/pq_l2_service.h>
#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

#include <library/cpp/streams/bzip2/bzip2.h>

using namespace std::chrono_literals;

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

namespace NPQ = NKikimr::NPQ;
namespace NConsole = NKikimr::NConsole;
namespace NSchemeCache = NKikimr::NSchemeCache;
namespace TEvKeyValue = NKikimr::TEvKeyValue;
namespace NMsgBusProxy = NKikimr::NMsgBusProxy;

const auto TEST_MESSAGE_GROUP_ID_1 = TEST_MESSAGE_GROUP_ID + "_1";
const auto TEST_MESSAGE_GROUP_ID_2 = TEST_MESSAGE_GROUP_ID + "_2";
const auto TEST_MESSAGE_GROUP_ID_3 = TEST_MESSAGE_GROUP_ID + "_3";
const auto TEST_MESSAGE_GROUP_ID_4 = TEST_MESSAGE_GROUP_ID + "_4";

TFixture::TTableRecord::TTableRecord(const std::string& key, const std::string& value) :
    Key(key),
    Value(value)
{
}

void TFixture::SetUp(NUnitTest::TTestContext&)
{
    NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetEnableTopicServiceTx(true);
    settings.SetEnableTopicSplitMerge(true);
    settings.SetEnablePQConfigTransactionsAtSchemeShard(true);
    settings.SetEnableOltpSink(GetEnableOltpSink());
    settings.SetEnableOlapSink(GetEnableOlapSink());
    settings.SetEnableHtapTx(GetEnableHtapTx());
    settings.SetAllowOlapDataQuery(GetAllowOlapDataQuery());

    Setup = std::make_unique<TTopicSdkTestSetup>(TEST_CASE_NAME, settings);

    Driver = std::make_unique<TDriver>(Setup->MakeDriver());
    auto tableSettings = NTable::TClientSettings().SessionPoolSettings(NTable::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    auto querySettings = NQuery::TClientSettings().SessionPoolSettings(NQuery::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    TableClient = std::make_unique<NTable::TTableClient>(*Driver, tableSettings);
    QueryClient = std::make_unique<NQuery::TQueryClient>(*Driver, querySettings);
}

void TFixture::NotifySchemeShard(const TFeatureFlags& flags)
{
    auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    *request->Record.MutableConfig() = *Setup->GetServer().ServerSettings.AppConfig;
    request->Record.MutableConfig()->MutableFeatureFlags()->SetEnablePQConfigTransactionsAtSchemeShard(flags.EnablePQConfigTransactionsAtSchemeShard);

    auto& runtime = Setup->GetRuntime();
    auto actorId = runtime.AllocateEdgeActor();

    std::uint64_t ssId = GetSchemeShardTabletId(actorId);

    runtime.SendToPipe(ssId, actorId, request.release());
    runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>();
}

TFixture::TTableSession::TTableSession(NTable::TTableClient& client)
    : Session_(Init(client))
{
}

NTable::TSession TFixture::TTableSession::Init(NTable::TTableClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

std::vector<TResultSet> TFixture::TTableSession::Execute(const std::string& query,
                                                         TTransactionBase* tx,
                                                         bool commit,
                                                         const TParams& params)
{
    while (true) {
        auto txTable = dynamic_cast<NTable::TTransaction*>(tx);
        auto txControl = NTable::TTxControl::Tx(*txTable).CommitTx(commit);

        auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return std::move(result).ExtractResultSets();
        }
        std::this_thread::sleep_for(100ms);
    }
}

TFixture::ISession::TExecuteInTxResult TFixture::TTableSession::ExecuteInTx(const std::string& query,
                                                                            bool commit,
                                                                            const TParams& params)
{
    while (true) {
        auto txControl = NTable::TTxControl::BeginTx().CommitTx(commit);

        auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return {std::move(result).ExtractResultSets(), std::make_unique<NTable::TTransaction>(*result.GetTransaction())};
        }
        std::this_thread::sleep_for(100ms);
    }
}

std::unique_ptr<TTransactionBase> TFixture::TTableSession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return std::make_unique<NTable::TTransaction>(result.GetTransaction());
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TTableSession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TTableSession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TTableSession::Close()
{
    Session_.Close();
}

TAsyncStatus TFixture::TTableSession::AsyncCommitTx(TTransactionBase& tx)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    return txTable.Commit().Apply([](auto result) {
        return TStatus(result.GetValue());
    });
}

TFixture::TQuerySession::TQuerySession(NQuery::TQueryClient& client,
                                       const std::string& endpoint,
                                       const std::string& database)
    : Session_(Init(client))
    , Endpoint_(endpoint)
    , Database_(database)
{
}

NQuery::TSession TFixture::TQuerySession::Init(NQuery::TQueryClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

std::vector<TResultSet> TFixture::TQuerySession::Execute(const std::string& query,
                                                         TTransactionBase* tx,
                                                         bool commit,
                                                         const TParams& params)
{
    while (true) {
        auto txQuery = dynamic_cast<NQuery::TTransaction*>(tx);
        auto txControl = NQuery::TTxControl::Tx(*txQuery).CommitTx(commit);

        auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetResultSets();
        }
        std::this_thread::sleep_for(100ms);
    }
}

TFixture::ISession::TExecuteInTxResult TFixture::TQuerySession::ExecuteInTx(const std::string& query,
                                                                            bool commit,
                                                                            const TParams& params)
{
    while (true) {
        auto txControl = NQuery::TTxControl::BeginTx().CommitTx(commit);

        auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return {result.GetResultSets(), std::make_unique<NQuery::TTransaction>(*result.GetTransaction())};
        }
        std::this_thread::sleep_for(100ms);
    }
}

std::unique_ptr<TTransactionBase> TFixture::TQuerySession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction(NQuery::TTxSettings()).ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return std::make_unique<NQuery::TTransaction>(result.GetTransaction());
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TQuerySession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TQuerySession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::TQuerySession::Close()
{
    // SDK doesn't provide a method to close the session for Query Client, so we use grpc API directly
    auto credentials = grpc::InsecureChannelCredentials();
    auto channel = grpc::CreateChannel(TStringType(Endpoint_), credentials);
    auto stub = Ydb::Query::V1::QueryService::NewStub(channel);

    grpc::ClientContext context;
    context.AddMetadata("x-ydb-database", TStringType(Database_));

    Ydb::Query::DeleteSessionRequest request;
    request.set_session_id(Session_.GetId());

    Ydb::Query::DeleteSessionResponse response;
    auto status = stub->DeleteSession(&context, request, &response);

    NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(response.issues(), issues);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, issues.ToString());
}

TAsyncStatus TFixture::TQuerySession::AsyncCommitTx(TTransactionBase& tx)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    return txQuery.Commit().Apply([](auto result) {
        return TStatus(result.GetValue());
    });
}

std::unique_ptr<TFixture::ISession> TFixture::CreateSession()
{
    switch (GetClientType()) {
        case EClientType::Table: {
            UNIT_ASSERT_C(TableClient, "TableClient is not initialized");
            return std::make_unique<TFixture::TTableSession>(*TableClient);
        }
        case EClientType::Query: {
            UNIT_ASSERT_C(QueryClient, "QueryClient is not initialized");
            return std::make_unique<TFixture::TQuerySession>(*QueryClient,
                                                             Setup->GetEndpoint(),
                                                             Setup->GetDatabase());
        }
        case EClientType::None: {
            UNIT_FAIL("CreateSession is forbidden for None client type");
        }
    }

    return nullptr;
}

template<class E>
E TFixture::ReadEvent(TTopicReadSessionPtr reader, TTransactionBase& tx)
{
    NTopic::TReadSessionGetEventSettings options;
    options.Block(true);
    options.MaxEventsCount(1);
    options.Tx(tx);

    auto event = reader->GetEvent(options);
    UNIT_ASSERT(event);

    auto ev = std::get_if<E>(&*event);
    UNIT_ASSERT(ev);

    return *ev;
}

template<class E>
E TFixture::ReadEvent(TTopicReadSessionPtr reader)
{
    auto event = reader->GetEvent(true, 1);
    UNIT_ASSERT(event);

    auto ev = std::get_if<E>(&*event);
    UNIT_ASSERT(ev);

    return *ev;
}

void TFixture::CreateTopic(const std::string& path,
                           const std::string& consumer,
                           std::size_t partitionCount,
                           std::optional<size_t> maxPartitionCount,
                           const TDuration retention,
                           bool important)
{
    Setup->CreateTopic(path, consumer, partitionCount, maxPartitionCount, retention, important);
}

void TFixture::AddConsumer(const std::string& topicPath,
                           const std::vector<std::string>& consumers)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    for (const auto& consumer : consumers) {
        settings.BeginAddConsumer(consumer);
    }

    auto result = client.AlterTopic(topicPath, settings).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void TFixture::SetPartitionWriteSpeed(const std::string& topicName, std::size_t bytesPerSeconds)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    settings.SetPartitionWriteSpeedBytesPerSecond(bytesPerSeconds);

    auto result = client.AlterTopic(Setup->GetTopicPath(topicName), settings).GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
}

const TDriver& TFixture::GetDriver() const
{
    return *Driver;
}

NTable::TTableClient& TFixture::GetTableClient()
{
    return *TableClient;
}

auto TFixture::CreateTopicWriteSession(const std::string& topicPath,
                                       const std::string& messageGroupId,
                                       std::optional<std::uint32_t> partitionId) -> TTopicWriteSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TWriteSessionSettings options;
    options.Path(topicPath);
    options.ProducerId(messageGroupId);
    options.MessageGroupId(messageGroupId);
    options.PartitionId(partitionId);
    options.Codec(ECodec::RAW);
    return client.CreateWriteSession(options);
}

auto TFixture::GetTopicWriteSession(const std::string& topicPath,
                                    const std::string& messageGroupId,
                                    std::optional<std::uint32_t> partitionId) -> TTopicWriteSessionContext&
{
    std::pair<std::string, std::string> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    if (i == TopicWriteSessions.end()) {
        TTopicWriteSessionContext context;
        context.Session = CreateTopicWriteSession(topicPath, messageGroupId, partitionId);

        TopicWriteSessions.emplace(key, std::move(context));

        i = TopicWriteSessions.find(key);
    }

    return i->second;
}

NTopic::TTopicReadSettings MakeTopicReadSettings(const std::string& topicPath,
                                                 std::optional<std::uint32_t> partitionId)
{
    TTopicReadSettings options;
    options.Path(topicPath);
    if (partitionId) {
        options.AppendPartitionIds(*partitionId);
    }
    return options;
}

NTopic::TReadSessionSettings MakeTopicReadSessionSettings(const std::string& topicPath,
                                                          const std::string& consumerName,
                                                          std::optional<std::uint32_t> partitionId)
{
    NTopic::TReadSessionSettings options;
    options.AppendTopics(MakeTopicReadSettings(topicPath, partitionId));
    options.ConsumerName(consumerName);
    return options;
}

auto TFixture::CreateTopicReadSession(const std::string& topicPath,
                                      const std::string& consumerName,
                                      std::optional<std::uint32_t> partitionId) -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    return client.CreateReadSession(MakeTopicReadSessionSettings(topicPath,
                                                                 consumerName,
                                                                 partitionId));
}

auto TFixture::GetTopicReadSession(const std::string& topicPath,
                                   const std::string& consumerName,
                                   std::optional<std::uint32_t> partitionId) -> TTopicReadSessionPtr
{
    TTopicReadSessionPtr session;

    if (auto i = TopicReadSessions.find(topicPath); i == TopicReadSessions.end()) {
        session = CreateTopicReadSession(topicPath, consumerName, partitionId);
        auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(session);
        event.Confirm();
        TopicReadSessions.emplace(topicPath, session);
    } else {
        session = i->second;
    }

    return session;
}

void TFixture::TTopicWriteSessionContext::WaitForContinuationToken()
{
    while (!ContinuationToken.has_value()) {
        WaitForEvent();
    }
}

void TFixture::TTopicWriteSessionContext::WaitForEvent()
{
    Session->WaitEvent().Wait();
    for (auto& event : Session->GetEvents()) {
        if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
            ContinuationToken = std::move(e->ContinuationToken);
        } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
            for (auto& ack : e->Acks) {
                switch (ack.State) {
                case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN:
                    ++WrittenAckCount;
                    break;
                case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX:
                    ++WrittenInTxAckCount;
                    break;
                default:
                    break;
                }
            }
        } else if ([[maybe_unused]] auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            UNIT_FAIL("");
        }
    }
}

void TFixture::TTopicWriteSessionContext::Write(const std::string& message, TTransactionBase* tx)
{
    NTopic::TWriteMessage params(message);

    if (tx) {
        params.Tx(*tx);
    }

    Session->Write(std::move(*ContinuationToken),
                   std::move(params));

    ++WriteCount;
    ContinuationToken = std::nullopt;
}

void TFixture::CloseTopicWriteSession(const std::string& topicPath,
                                      const std::string& messageGroupId,
                                      bool force)
{
    std::pair<std::string, std::string> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    UNIT_ASSERT(i != TopicWriteSessions.end());

    TTopicWriteSessionContext& context = i->second;

    context.Session->Close(force ? TDuration::MilliSeconds(0) : TDuration::Max());
    TopicWriteSessions.erase(key);
}

void TFixture::CloseTopicReadSession(const std::string& topicPath,
                                     const std::string& consumerName)
{
    Y_UNUSED(consumerName);
    TopicReadSessions.erase(topicPath);
}

void TFixture::WriteToTopic(const std::string& topicPath,
                            const std::string& messageGroupId,
                            const std::string& message,
                            TTransactionBase* tx,
                            std::optional<std::uint32_t> partitionId)
{
    TTopicWriteSessionContext& context = GetTopicWriteSession(topicPath, messageGroupId, partitionId);
    context.WaitForContinuationToken();
    UNIT_ASSERT(context.ContinuationToken.has_value());
    context.Write(message, tx);
}

std::vector<std::string> TFixture::ReadFromTopic(const std::string& topicPath,
                                         const std::string& consumerName,
                                         const TDuration& duration,
                                         TTransactionBase* tx,
                                         std::optional<std::uint32_t> partitionId)
{
    std::vector<std::string> messages;

    TInstant end = TInstant::Now() + duration;
    TDuration remain = duration;

    auto session = GetTopicReadSession(topicPath, consumerName, partitionId);

    while (TInstant::Now() < end) {
        if (!session->WaitEvent().Wait(remain)) {
            return messages;
        }

        NTopic::TReadSessionGetEventSettings settings;
        if (tx) {
            settings.Tx(*tx);
        }

        for (auto& event : session->GetEvents(settings)) {
            if (auto* e = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                std::cerr << e->HasCompressedMessages() << " " << e->GetMessagesCount() << std::endl;
                for (auto& m : e->GetMessages()) {
                    messages.emplace_back(m.GetData());
                }

                if (!tx) {
                    e->Commit();
                }
            }
        }

        remain = end - TInstant::Now();
    }

    return messages;
}

void TFixture::WaitForAcks(const std::string& topicPath, const std::string& messageGroupId, std::size_t writtenInTxCount)
{
    std::pair<std::string, std::string> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);
    UNIT_ASSERT(i != TopicWriteSessions.end());

    auto& context = i->second;

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);

    while (context.AckCount() < context.WriteCount) {
        context.WaitForEvent();
    }

    UNIT_ASSERT((context.WrittenAckCount + context.WrittenInTxAckCount) == context.WriteCount);

    if (writtenInTxCount != std::numeric_limits<std::size_t>::max()) {
        UNIT_ASSERT_VALUES_EQUAL(context.WrittenInTxAckCount, writtenInTxCount);
    }
}

void TFixture::WaitForSessionClose(const std::string& topicPath,
                                   const std::string& messageGroupId,
                                   NYdb::EStatus status)
{
    std::pair<std::string, std::string> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);
    UNIT_ASSERT(i != TopicWriteSessions.end());

    auto& context = i->second;

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);

    for(bool stop = false; !stop; ) {
        context.Session->WaitEvent().Wait();
        for (auto& event : context.Session->GetEvents()) {
            if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                context.ContinuationToken = std::move(e->ContinuationToken);
            } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
                for (auto& ack : e->Acks) {
                    switch (ack.State) {
                    case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN:
                        ++context.WrittenAckCount;
                        break;
                    case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX:
                        ++context.WrittenInTxAckCount;
                        break;
                    default:
                        break;
                    }
                }
            } else if (auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                UNIT_ASSERT_VALUES_EQUAL(e->GetStatus(), status);
                UNIT_ASSERT_GT(e->GetIssues().Size(), 0);
                stop = true;
            }
        }
    }

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);
}

std::uint64_t TFixture::GetSchemeShardTabletId(const NActors::TActorId& actorId)
{
    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath("/Root");
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

    navigate->ResultSet.push_back(std::move(entry));
    //navigate->UserToken = "root@builtin";
    navigate->Cookie = 12345;

    auto& runtime = Setup->GetRuntime();

    runtime.Send(NKikimr::MakeSchemeCacheID(), actorId,
                 new NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
                 0,
                 true);
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult>();

    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0);

    auto& front = response->Request->ResultSet.front();

    return front.Self->Info.GetSchemeshardId();
}

std::uint64_t TFixture::GetTopicTabletId(const NActors::TActorId& actorId, const std::string& topicPath, std::uint32_t partition)
{
    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(TString{topicPath});
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

    navigate->ResultSet.push_back(std::move(entry));
    //navigate->UserToken = "root@builtin";
    navigate->Cookie = 12345;

    auto& runtime = Setup->GetRuntime();

    runtime.Send(NKikimr::MakeSchemeCacheID(), actorId,
                 new NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
                 0,
                 true);
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult>();

    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    UNIT_ASSERT_GT(front.PQGroupInfo->Description.PartitionsSize(), 0);
    UNIT_ASSERT_LT(partition, front.PQGroupInfo->Description.PartitionsSize());

    for (size_t i = 0; i < front.PQGroupInfo->Description.PartitionsSize(); ++i) {
        auto& p = front.PQGroupInfo->Description.GetPartitions(partition);
        if (p.GetPartitionId() == partition) {
            return p.GetTabletId();
        }
    }

    UNIT_FAIL("unknown partition");

    return std::numeric_limits<std::uint64_t>::max();
}

std::vector<std::string> TFixture::GetTabletKeys(const NActors::TActorId& actorId,
                                                 std::uint64_t tabletId)
{
    auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);

    auto cmd = request->Record.AddCmdReadRange();
    TString from(1, '\x00');
    TString to(1, '\xFF');
    auto range = cmd->MutableRange();
    range->SetFrom(from);
    range->SetIncludeFrom(true);
    range->SetTo(to);
    range->SetIncludeTo(true);

    auto& runtime = Setup->GetRuntime();

    runtime.SendToPipe(tabletId, actorId, request.release());
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

    UNIT_ASSERT(response->Record.HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadRangeResultSize(), 1);

    std::vector<std::string> keys;

    auto& result = response->Record.GetReadRangeResult(0);
    for (size_t i = 0; i < result.PairSize(); ++i) {
        auto& kv = result.GetPair(i);
        keys.emplace_back(kv.GetKey());
    }

    return keys;
}

size_t TFixture::GetPQCacheRenameKeysCount()
{
    using namespace NKikimr::NPQ;

    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();

    auto request = MakeHolder<TEvPqCache::TEvCacheKeysRequest>();

    runtime.Send(MakePersQueueL2CacheID(), edge, request.Release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto* result = runtime.GrabEdgeEvent<TEvPqCache::TEvCacheKeysResponse>(handle);

    return result->RenamedKeys;
}

std::vector<std::string> TFixture::Read_Exactly_N_Messages_From_Topic(const std::string& topicPath,
                                                                      const std::string& consumerName,
                                                                      std::size_t limit)
{
    std::vector<std::string> result;

    while (result.size() < limit) {
        auto messages = ReadFromTopic(topicPath, consumerName, TDuration::Seconds(2));
        for (auto& m : messages) {
            result.push_back(std::move(m));
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(result.size(), limit);

    return result;
}

void TFixture::TestWriteToTopic1()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #4", tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #5", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #6", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #7", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #8", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #9", tx.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #4");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 5);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #5");
        UNIT_ASSERT_VALUES_EQUAL(messages[4], "message #9");
    }
}

void TFixture::TestWriteToTopic4()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx_1 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx_1.get());

    auto tx_2 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", tx_2.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #4", tx_2.get());

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #4");
}

void TFixture::TestWriteToTopic7()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #2", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #3");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #4");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #5", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #6", tx.get());

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #4");
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #6");
    }
}

void TFixture::TestWriteToTopic9()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx_1 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());

    auto tx_2 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx_2.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
    }
}

void TFixture::TestWriteToTopic10()
{
    CreateTopic("topic_A");

    auto session = CreateSession();

    {
        auto tx_1 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());

        session->CommitTx(*tx_1, EStatus::SUCCESS);
    }

    {
        auto tx_2 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx_2.get());

        session->CommitTx(*tx_2, EStatus::SUCCESS);
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
    }
}

void TFixture::TestWriteToTopic11()
{
    for (auto endOfTransaction : {Commit, Rollback, CloseTableSession}) {
        TestTheCompletionOfATransaction({.Topics={"topic_A"}, .EndOfTransaction = endOfTransaction});
        TestTheCompletionOfATransaction({.Topics={"topic_A", "topic_B"}, .EndOfTransaction = endOfTransaction});
    }
}

void TFixture::TestWriteToTopic24()
{
    //
    // the test verifies a transaction in which data is written to a topic and to a table
    //
    CreateTopic("topic_A");
    CreateTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], MakeJsonDoc(records));

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

void TFixture::TestWriteToTopic26()
{
    //
    // the test verifies a transaction in which data is read from a partition of one topic and written to
    // another partition of this topic
    //
    const std::uint32_t PARTITION_0 = 0;
    const std::uint32_t PARTITION_1 = 1;

    CreateTopic("topic_A", TEST_CONSUMER, 2);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", nullptr, PARTITION_0);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), PARTITION_0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

    for (const auto& m : messages) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, m, tx.get(), PARTITION_1);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, PARTITION_1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
}

void TFixture::TestWriteToTopic27()
{
    CreateTopic("topic_A", TEST_CONSUMER);
    CreateTopic("topic_B", TEST_CONSUMER);
    CreateTopic("topic_C", TEST_CONSUMER);

    for (size_t i = 0; i < 2; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, 0);
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, 0);

        auto session = CreateSession();
        auto tx = session->BeginTx();

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);

        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], tx.get(), 0);

        messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);

        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], tx.get(), 0);

        session->CommitTx(*tx, EStatus::SUCCESS);

        messages = ReadFromTopic("topic_C", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        DumpPQTabletKeys("topic_A");
        DumpPQTabletKeys("topic_B");
        DumpPQTabletKeys("topic_C");
    }
}

auto TFixture::GetAvgWriteBytes(const std::string& topicName,
                                std::uint32_t partitionId) -> TAvgWriteBytes
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partitionId);

    runtime.SendToPipe(tabletId, edge, new NKikimr::TEvPersQueue::TEvStatus());
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvPersQueue::TEvStatusResponse>();

    UNIT_ASSERT_VALUES_EQUAL(tabletId, response->Record.GetTabletId());

    TAvgWriteBytes result;

    for (std::size_t i = 0; i < response->Record.PartResultSize(); ++i) {
        const auto& partition = response->Record.GetPartResult(i);
        if (partition.GetPartition() == static_cast<int>(partitionId)) {
            result.PerSec = partition.GetAvgWriteSpeedPerSec();
            result.PerMin = partition.GetAvgWriteSpeedPerMin();
            result.PerHour = partition.GetAvgWriteSpeedPerHour();
            result.PerDay = partition.GetAvgWriteSpeedPerDay();
            break;
        }
    }

    return result;
}

bool TFixture::GetEnableOltpSink() const
{
    return false;
}

bool TFixture::GetEnableOlapSink() const
{
    return false;
}

bool TFixture::GetEnableHtapTx() const
{
    return false;
}

bool TFixture::GetAllowOlapDataQuery() const
{
    return false;
}

NPQ::TWriteId TFixture::GetTransactionWriteId(const NActors::TActorId& actorId,
                                              std::uint64_t tabletId)
{
    auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    request->Record.AddCmdRead()->SetKey("_txinfo");

    auto& runtime = Setup->GetRuntime();

    runtime.SendToPipe(tabletId, actorId, request.release());
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

    UNIT_ASSERT(response->Record.HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadResultSize(), 1);

    auto& read = response->Record.GetReadResult(0);

    NKikimrPQ::TTabletTxInfo info;
    UNIT_ASSERT(info.ParseFromString(read.GetValue()));

    UNIT_ASSERT_VALUES_EQUAL(info.TxWritesSize(), 1);

    auto& writeInfo = info.GetTxWrites(0);
    UNIT_ASSERT(writeInfo.HasWriteId());

    return NPQ::GetWriteId(writeInfo);
}

void TFixture::SendLongTxLockStatus(const NActors::TActorId& actorId,
                                    std::uint64_t tabletId,
                                    const NPQ::TWriteId& writeId,
                                    NKikimrLongTxService::TEvLockStatus::EStatus status)
{
    auto event =
        std::make_unique<NKikimr::NLongTxService::TEvLongTxService::TEvLockStatus>(writeId.KeyId, writeId.NodeId,
                                                                                   status);
    auto& runtime = Setup->GetRuntime();
    runtime.SendToPipe(tabletId, actorId, event.release());
}

void TFixture::WaitForTheTabletToDeleteTheWriteInfo(const NActors::TActorId& actorId,
                                                    std::uint64_t tabletId,
                                                    const NPQ::TWriteId& writeId)
{
    while (true) {
        auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        request->Record.AddCmdRead()->SetKey("_txinfo");

        auto& runtime = Setup->GetRuntime();

        runtime.SendToPipe(tabletId, actorId, request.release());
        auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

        UNIT_ASSERT(response->Record.HasCookie());
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadResultSize(), 1);

        auto& read = response->Record.GetReadResult(0);

        NKikimrPQ::TTabletTxInfo info;
        UNIT_ASSERT(info.ParseFromString(read.GetValue()));

        bool found = false;

        for (size_t i = 0; i < info.TxWritesSize(); ++i) {
            auto& writeInfo = info.GetTxWrites(i);
            UNIT_ASSERT(writeInfo.HasWriteId());
            if ((NPQ::GetWriteId(writeInfo) == writeId) && writeInfo.HasOriginalPartitionId()) {
                found = true;
                break;
            }
        }

        if (!found) {
            break;
        }

        std::this_thread::sleep_for(100ms);
    }
}

void TFixture::RestartPQTablet(const std::string& topicName, std::uint32_t partition)
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partition);
    runtime.SendToPipe(tabletId, edge, new NActors::TEvents::TEvPoison());

    std::this_thread::sleep_for(2s);
}

void TFixture::DeleteSupportivePartition(const std::string& topicName, std::uint32_t partition)
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partition);
    NPQ::TWriteId writeId = GetTransactionWriteId(edge, tabletId);

    SendLongTxLockStatus(edge, tabletId, writeId, NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);

    WaitForTheTabletToDeleteTheWriteInfo(edge, tabletId, writeId);
}

void TFixture::CheckTabletKeys(const std::string& topicName)
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicName, 0);

    const std::unordered_set<char> types {
        NPQ::TKeyPrefix::TypeInfo,
        NPQ::TKeyPrefix::TypeData,
        NPQ::TKeyPrefix::TypeTmpData,
        NPQ::TKeyPrefix::TypeMeta,
        NPQ::TKeyPrefix::TypeTxMeta,
    };

    bool found;
    std::vector<std::string> keys;
    for (size_t i = 0; i < 20; ++i) {
        keys = GetTabletKeys(edge, tabletId);

        found = false;
        for (const auto& key : keys) {
            UNIT_ASSERT_GT(key.size(), 0);
            if (key[0] == '_') {
                continue;
            }

            if (types.contains(key[0])) {
                found = false;
                break;
            }
        }

        if (!found) {
            break;
        }

        std::this_thread::sleep_for(100ms);
    }

    if (found) {
        std::cerr << "keys for tablet " << tabletId << ":" << std::endl;
        for (const auto& k : keys) {
            std::cerr << k << std::endl;
        }
        std::cerr << "=============" << std::endl;

        UNIT_FAIL("unexpected keys for tablet " << tabletId);
    }
}

void TFixture::DumpPQTabletKeys(const std::string& topicName)
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicName, 0);
    auto keys = GetTabletKeys(edge, tabletId);
    for (const auto& key : keys) {
        std::cerr << key << std::endl;
    }
}

void TFixture::PQTabletPrepareFromResource(const std::string& topicPath,
                                           std::uint32_t partitionId,
                                           const std::string& resourceName)
{
    auto& runtime = Setup->GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();
    std::uint64_t tabletId = GetTopicTabletId(edge, "/Root/" + topicPath, partitionId);

    auto request = MakeHolder<TEvKeyValue::TEvRequest>();
    std::size_t count = 0;

    for (TStringStream stream(NResource::Find(resourceName)); true; ++count) {
        TString key, encoded;

        if (!stream.ReadTo(key, ' ')) {
            break;
        }
        encoded = stream.ReadLine();

        auto decoded = Base64Decode(encoded);
        TStringInput decodedStream(decoded);
        TBZipDecompress decompressor(&decodedStream);

        auto* cmd = request->Record.AddCmdWrite();
        cmd->SetKey(key);
        cmd->SetValue(decompressor.ReadAll());
    }

    runtime.SendToPipe(tabletId, edge, request.Release(), 0, NKikimr::GetPipeConfigWithRetries());

    TAutoPtr<NActors::IEventHandle> handle;
    auto* response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.HasStatus());
    UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

    UNIT_ASSERT_VALUES_EQUAL(response->Record.WriteResultSize(), count);

    for (std::size_t i = 0; i < response->Record.WriteResultSize(); ++i) {
        const auto &result = response->Record.GetWriteResult(i);
        UNIT_ASSERT(result.HasStatus());
        UNIT_ASSERT_EQUAL(result.GetStatus(), NKikimrProto::OK);
    }
}

void TFixture::TestTheCompletionOfATransaction(const TTransactionCompletionTestDescription& d)
{
    for (auto& topic : d.Topics) {
        CreateTopic(topic);
    }

    {
        auto session = CreateSession();
        auto tx = session->BeginTx();

        for (auto& topic : d.Topics) {
            WriteToTopic(topic, TEST_MESSAGE_GROUP_ID, "message", tx.get());
            // TODO: нужен callback для RollbakTx
            WaitForAcks(topic, TEST_MESSAGE_GROUP_ID);
        }

        switch (d.EndOfTransaction) {
        case Commit:
            session->CommitTx(*tx, EStatus::SUCCESS);
            break;
        case Rollback:
            session->RollbackTx(*tx, EStatus::SUCCESS);
            break;
        case CloseTableSession:
            break;
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));

    for (auto& topic : d.Topics) {
        CheckTabletKeys(topic);
    }

    for (auto& topic : d.Topics) {
        CloseTopicWriteSession(topic, TEST_MESSAGE_GROUP_ID);
    }
}

void TFixture::TestWriteToTopic12()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WaitForSessionClose("topic_A", TEST_MESSAGE_GROUP_ID, NYdb::EStatus::PRECONDITION_FAILED);
}

void TFixture::TestWriteToTopic13()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    session->CommitTx(*tx, EStatus::ABORTED);
}

void TFixture::TestWriteToTopic14()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    session->CommitTx(*tx, EStatus::ABORTED);
}

void TFixture::TestWriteToTopic16()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    RestartPQTablet("topic_A", 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
}

void TFixture::TestTxWithBigBlobs(const TTestTxWithBigBlobsParams& params)
{
    size_t oldHeadMsgCount = 0;
    size_t bigBlobMsgCount = 0;
    size_t newHeadMsgCount = 0;

    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t i = 0; i < params.OldHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(100'000, 'x'));
        ++oldHeadMsgCount;
    }

    for (size_t i = 0; i < params.BigBlobsCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(7'000'000, 'x'), tx.get());
        ++bigBlobMsgCount;
    }

    for (size_t i = 0; i < params.NewHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(100'000, 'x'), tx.get());
        ++newHeadMsgCount;
    }

    if (params.RestartMode == ERestartBeforeCommit) {
        RestartPQTablet("topic_A", 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    if (params.RestartMode == ERestartAfterCommit) {
        RestartPQTablet("topic_A", 0);
    }

    std::vector<std::string> messages;
    for (size_t i = 0; (i < 10) && (messages.size() < (oldHeadMsgCount + bigBlobMsgCount + newHeadMsgCount)); ++i) {
        auto block = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        for (auto& m : block) {
            messages.push_back(std::move(m));
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(messages.size(), oldHeadMsgCount + bigBlobMsgCount + newHeadMsgCount);

    size_t start = 0;

    for (size_t i = 0; i < oldHeadMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 100'000);
    }
    start += oldHeadMsgCount;

    for (size_t i = 0; i < bigBlobMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 7'000'000);
    }
    start += bigBlobMsgCount;

    for (size_t i = 0; i < newHeadMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 100'000);
    }
}

void TFixture::CreateTable(const std::string& tablePath)
{
    UNIT_ASSERT(!tablePath.empty());

    std::string path = (tablePath[0] != '/') ? ("/Root/" + tablePath) : tablePath;

    auto createSessionResult = GetTableClient().CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
    auto session = createSessionResult.GetSession();

    auto desc = NTable::TTableBuilder()
        .AddNonNullableColumn("key", EPrimitiveType::Utf8)
        .AddNonNullableColumn("value", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("key")
        .Build();
    auto result = session.CreateTable(path, std::move(desc)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

auto TFixture::MakeTableRecords() -> std::vector<TTableRecord>
{
    std::vector<TTableRecord> records;
    records.emplace_back("key-1", "value-1");
    records.emplace_back("key-2", "value-2");
    records.emplace_back("key-3", "value-3");
    records.emplace_back("key-4", "value-4");
    return records;
}

auto TFixture::MakeJsonDoc(const std::vector<TTableRecord>& records) -> std::string
{
    auto makeJsonObject = [](const TTableRecord& r) {
        return Sprintf(R"({"key":"%s", "value":"%s"})",
                       r.Key.data(),
                       r.Value.data());
    };

    if (records.empty()) {
        return "[]";
    }

    std::string s = "[";

    s += makeJsonObject(records.front());
    for (auto i = records.begin() + 1; i != records.end(); ++i) {
        s += ", ";
        s += makeJsonObject(*i);
    }
    s += "]";

    return s;
}

void TFixture::UpsertToTable(const std::string& tablePath,
                             const std::vector<TTableRecord>& records,
                             ISession& session,
                             TTransactionBase* tx)
{
    auto query = Sprintf("DECLARE $key AS Utf8;"
                         "DECLARE $value AS Utf8;"
                         "UPSERT INTO `%s` (key, value) VALUES ($key, $value);",
                         tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

void TFixture::InsertToTable(const std::string& tablePath,
                             const std::vector<TTableRecord>& records,
                             ISession& session,
                             TTransactionBase* tx)
{
    auto query = Sprintf("DECLARE $key AS Utf8;"
                         "DECLARE $value AS Utf8;"
                         "INSERT INTO `%s` (key, value) VALUES ($key, $value);",
                         tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

void TFixture::DeleteFromTable(const std::string& tablePath,
                            const std::vector<TTableRecord>& records,
                            ISession& session,
                            TTransactionBase* tx)
{
    auto query = Sprintf("DECLARE $key AS Utf8;"
                         "DECLARE $value AS Utf8;"
                         "DELETE FROM `%s` ON (key, value) VALUES ($key, $value);",
                         tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

std::size_t TFixture::GetTableRecordsCount(const std::string& tablePath)
{
    auto query = Sprintf(R"(SELECT COUNT(*) FROM `%s`)",
                         tablePath.data());
    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto result = session->Execute(query, tx.get());

    NYdb::TResultSetParser parser(result.at(0));
    UNIT_ASSERT(parser.TryNextRow());

    return parser.ColumnParser(0).GetUint64();
}

void TFixture::WriteMessagesInTx(std::size_t big, std::size_t small)
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t i = 0; i < big; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(7'000'000, 'x'), tx.get(), 0);
    }

    for (std::size_t i = 0; i < small; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(16'384, 'x'), tx.get(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);
}

void TFixture::TestWriteToTopic38()
{
    WriteMessagesInTx(2, 202);
    WriteMessagesInTx(2, 200);
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(4, 0);
    WriteMessagesInTx(0, 1);
}

void TFixture::TestWriteToTopic40()
{
    // The recording stream will run into a quota. Before the commit, the client will receive confirmations
    // for some of the messages. The `CommitTx` call will wait for the rest.
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(1'000'000, 'a'), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

void TFixture::TestWriteToTopic41()
{
    // If the recording session does not wait for confirmations, the commit will fail
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(1'000'000, 'a'), tx.get());
    }

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID, true); // force close

    session->CommitTx(*tx, EStatus::SESSION_EXPIRED);
}

void TFixture::TestWriteToTopic42()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(1'000'000, 'a'), tx.get());
    }

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID); // gracefully close

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

void TFixture::TestWriteToTopic43()
{
    // The recording stream will run into a quota. Before the commit, the client will receive confirmations
    // for some of the messages. The `ExecuteDataQuery` call will wait for the rest.
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(1'000'000, 'a'), tx.get());
    }

    session->Execute("SELECT 1", tx.get());

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

void TFixture::TestWriteToTopic44()
{
    CreateTopic("topic_A");

    auto session = CreateSession();

    auto [_, tx] = session->ExecuteInTx("SELECT 1", false);

    for (std::size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(1'000'000, 'a'), tx.get());
    }

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(60));
    UNIT_ASSERT_EQUAL(messages.size(), 0u);

    session->Execute("SELECT 2", tx.get());

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

void TFixture::CheckAvgWriteBytes(const std::string& topicPath,
                                  std::uint32_t partitionId,
                                  std::size_t minSize, std::size_t maxSize)
{
#define UNIT_ASSERT_AVGWRITEBYTES(v, minSize, maxSize) \
    UNIT_ASSERT_LE_C(minSize, v, ", actual " << minSize << " > " << v); \
    UNIT_ASSERT_LE_C(v, maxSize, ", actual " << v << " > " << maxSize);

    auto avgWriteBytes = GetAvgWriteBytes(topicPath, partitionId);

    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerSec, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerMin, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerHour, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerDay, minSize, maxSize);

#undef UNIT_ASSERT_AVGWRITEBYTES
}

void TFixture::SplitPartition(const std::string& topicName,
                              std::uint32_t partitionId,
                              const std::string& boundary)
{
    NKikimr::NPQ::NTest::SplitPartition(Setup->GetRuntime(),
                                        ++SchemaTxId,
                                        TString(topicName),
                                        partitionId,
                                        TString(boundary));
}

void TFixture::TestWriteToTopic45()
{
    // Writing to a topic in a transaction affects the `AvgWriteBytes` indicator
    CreateTopic("topic_A", TEST_CONSUMER, 2);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    std::string message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::SUCCESS);

    std::size_t minSize = (message.size() + TEST_MESSAGE_GROUP_ID_1.size()) * 2;
    std::size_t maxSize = minSize + 200;

    CheckAvgWriteBytes("topic_A", 0, minSize, maxSize);

    minSize = (message.size() + TEST_MESSAGE_GROUP_ID_2.size());
    maxSize = minSize + 200;

    CheckAvgWriteBytes("topic_A", 1, minSize, maxSize);
}

void TFixture::TestWriteToTopic46()
{
    // The `split` operation of the topic partition affects the writing in the transaction.
    // The transaction commit should fail with an error
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    std::string message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    SplitPartition("topic_A", 1, "\xC0");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::ABORTED);
}

void TFixture::TestWriteToTopic47()
{
    // The `split` operation of the topic partition does not affect the reading in the transaction.
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);

    std::string message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, nullptr, 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, nullptr, 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, nullptr, 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, nullptr, 1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    SplitPartition("topic_A", 1, "\xC0");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

    CloseTopicReadSession("topic_A", TEST_CONSUMER);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

    session->CommitTx(*tx, EStatus::SUCCESS);
}

void TFixture::TestWriteRandomSizedMessagesInWideTransactions()
{
    // The test verifies the simultaneous execution of several transactions. There is a topic
    // with PARTITIONS_COUNT partitions. In each transaction, the test writes to all the partitions.
    // The size of the messages is random. Such that both large blobs in the body and small ones in
    // the head of the partition are obtained. Message sizes are multiples of 500 KB. This way we
    // will make sure that when committing transactions, the division into blocks is taken into account.

    const std::size_t PARTITIONS_COUNT = 20;
    const std::size_t TXS_COUNT = 10;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    // We open TXS_COUNT transactions and write messages to the topic.
    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (std::size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            std::string sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(i);
            sourceId += "_";
            sourceId += ToString(j);

            std::size_t count = RandomNumber<std::size_t>(20) + 3;
            WriteToTopic("topic_A", sourceId, std::string(512 * 1000 * count, 'x'), tx.get(), j);

            WaitForAcks("topic_A", sourceId);
        }
    }

    // We are doing an asynchronous commit of transactions. They will be executed simultaneously.
    std::vector<TAsyncStatus> futures;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // All transactions must be completed successfully.
    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void TFixture::TestWriteOnlyBigMessagesInWideTransactions()
{
    // The test verifies the simultaneous execution of several transactions. There is a topic `topic_A` and
    // it contains a `PARTITIONS_COUNT' of partitions. In each transaction, the test writes to all partitions.
    // The size of the messages is chosen so that only large blobs are recorded in the transaction and there
    // are no records in the head. Thus, we verify that transaction bundling is working correctly.

    const std::size_t PARTITIONS_COUNT = 20;
    const std::size_t TXS_COUNT = 100;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    // We open TXS_COUNT transactions and write messages to the topic.
    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (std::size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            std::string sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(i);
            sourceId += "_";
            sourceId += ToString(j);

            WriteToTopic("topic_A", sourceId, std::string(6'500'000, 'x'), tx.get(), j);

            WaitForAcks("topic_A", sourceId);
        }
    }

    // We are doing an asynchronous commit of transactions. They will be executed simultaneously.
    std::vector<TAsyncStatus> futures;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // All transactions must be completed successfully.
    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void TFixture::TestTransactionsConflictOnSeqNo()
{
    const std::uint32_t PARTITIONS_COUNT = 20;
    const std::size_t TXS_COUNT = 100;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    auto session = CreateSession();
    std::vector<std::shared_ptr<NTopic::ISimpleBlockingWriteSession>> topicWriteSessions;

    for (std::uint32_t i = 0; i < PARTITIONS_COUNT; ++i) {
        std::string sourceId = TEST_MESSAGE_GROUP_ID;
        sourceId += "_";
        sourceId += ToString(i);

        NTopic::TTopicClient client(GetDriver());
        NTopic::TWriteSessionSettings options;
        options.Path(Setup->GetTopicPath("topic_A"));
        options.ProducerId(sourceId);
        options.MessageGroupId(sourceId);
        options.PartitionId(i);
        options.Codec(ECodec::RAW);

        auto session = client.CreateSimpleBlockingWriteSession(options);

        topicWriteSessions.push_back(std::move(session));
    }

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (std::size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            std::string sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(j);

            for (std::size_t k = 0, count = RandomNumber<std::size_t>(20) + 1; k < count; ++k) {
                const std::string data(RandomNumber<std::size_t>(1'000) + 100, 'x');
                NTopic::TWriteMessage params(data);
                params.Tx(*tx);

                topicWriteSessions[j]->Write(std::move(params));
            }
        }
    }

    std::vector<TAsyncStatus> futures;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // Some transactions should end with the error `ABORTED`
    std::size_t successCount = 0;

    for (std::size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        switch (result.GetStatus()) {
        case EStatus::SUCCESS:
            ++successCount;
            break;
        case EStatus::ABORTED:
            break;
        default:
            UNIT_FAIL("unexpected status: " + ToString(static_cast<TStatus>(result)));
            break;
        }
    }

    UNIT_ASSERT_UNEQUAL(successCount, TXS_COUNT);
}

void TFixtureSinks::CreateRowTable(const std::string& path)
{
    CreateTable(path);
}

void TFixtureSinks::CreateColumnTable(const std::string& tablePath)
{
    UNIT_ASSERT(!tablePath.empty());

    std::string path = (tablePath[0] != '/') ? ("/Root/" + tablePath) : tablePath;

    auto createSessionResult = GetTableClient().CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
    auto session = createSessionResult.GetSession();

    auto desc = NTable::TTableBuilder()
        .SetStoreType(NTable::EStoreType::Column)
        .AddNonNullableColumn("key", EPrimitiveType::Utf8)
        .AddNonNullableColumn("value", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("key")
        .Build();
    auto result = session.CreateTable(path, std::move(desc)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

bool TFixtureSinks::GetEnableOltpSink() const
{
    return true;
}

bool TFixtureSinks::GetEnableOlapSink() const
{
    return true;
}

bool TFixtureSinks::GetEnableHtapTx() const
{
    return true;
}

bool TFixtureSinks::GetAllowOlapDataQuery() const
{
    return true;
}

void TFixtureSinks::TestSinksOltpWriteToTopic5()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable2()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #3");
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable3()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateRowTable("/Root/table_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    UpsertToTable("table_B", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable4()
{
    CreateTopic("topic_A");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx1 = session->BeginTx();
    auto tx2 = session->BeginTx();

    session->Execute(R"(SELECT COUNT(*) FROM `table_A`)", tx1.get(), false);

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx2.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx1.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->CommitTx(*tx2, EStatus::SUCCESS);
    session->CommitTx(*tx1, EStatus::ABORTED);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable5()
{
    CreateTopic("topic_A");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable6()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    InsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());

    DeleteFromTable("table_A", records, *session, tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #3");
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable1()
{
    CreateTopic("topic_A");
    CreateColumnTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable2()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateColumnTable("/Root/table_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();

    UpsertToTable("table_A", records, *session, tx.get());
    UpsertToTable("table_B", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable3()
{
    CreateTopic("topic_A");
    CreateColumnTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable4()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateColumnTable("/Root/table_B");
    CreateColumnTable("/Root/table_C");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();

    InsertToTable("table_A", records, *session, tx.get());
    InsertToTable("table_B", records, *session, tx.get());
    UpsertToTable("table_C", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    DeleteFromTable("table_B", records, *session, tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_C"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

void TFixture::TestWriteAndReadMessages(size_t count, size_t size, bool restart)
{
    CreateTopic("topic_A");

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    for (size_t i = 0; i < count; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(size, 'x'), nullptr);
    }
    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID);

    if (restart) {
        RestartPQTablet("topic_A", 0);
    }

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, count);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), count);
}

}
