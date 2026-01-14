#include "write_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>

namespace NYdb::inline Dev::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSession

TWriteSession::TWriteSession(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TTopicClient::TImpl> client,
         std::shared_ptr<TGRpcConnectionsImpl> connections,
         TDbDriverStatePtr dbDriverState)
    : TContextOwner(settings, std::move(client), std::move(connections), std::move(dbDriverState)) {
}

void TWriteSession::Start(const TDuration& delay) {
    TryGetImpl()->Start(delay);
}

NThreading::TFuture<uint64_t> TWriteSession::GetInitSeqNo() {
    return TryGetImpl()->GetInitSeqNo();
}

std::optional<TWriteSessionEvent::TEvent> TWriteSession::GetEvent(bool block) {
    return TryGetImpl()->EventsQueue->GetEvent(block);
}

std::vector<TWriteSessionEvent::TEvent> TWriteSession::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    return TryGetImpl()->EventsQueue->GetEvents(block, maxEventsCount);
}

NThreading::TFuture<void> TWriteSession::WaitEvent() {
    return TryGetImpl()->EventsQueue->WaitEvent();
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, std::string_view data, ECodec codec, ui32 originalSize,
                                 std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) {
    auto message = TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::WriteEncoded(TContinuationToken&& token, TWriteMessage&& message,
                                 TTransactionBase* tx)
{
    if (tx) {
        message.Tx(*tx);
    }
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, std::string_view data, std::optional<uint64_t> seqNo,
                          std::optional<TInstant> createTimestamp) {
    TWriteMessage message{std::move(data)};
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

void TWriteSession::Write(TContinuationToken&& token, TWriteMessage&& message,
                          TTransactionBase* tx) {
    if (tx) {
        message.Tx(*tx);
    }
    TryGetImpl()->WriteInternal(std::move(token), std::move(message));
}

bool TWriteSession::Close(TDuration closeTimeout) {
    return TryGetImpl()->Close(closeTimeout);
}

TWriteSession::~TWriteSession() {
    TryGetImpl()->Close(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingWriteSession

TSimpleBlockingWriteSession::TSimpleBlockingWriteSession(
        const TWriteSessionSettings& settings,
        std::shared_ptr<TTopicClient::TImpl> client,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbDriverState
) {
    auto subSettings = settings;
    if (settings.EventHandlers_.AcksHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use AcksHandler, resetting.");
        subSettings.EventHandlers_.AcksHandler({});
    }
    if (settings.EventHandlers_.ReadyToAcceptHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use ReadyToAcceptHandler, resetting.");
        subSettings.EventHandlers_.ReadyToAcceptHandler({});
    }
    if (settings.EventHandlers_.SessionClosedHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use SessionClosedHandler, resetting.");
        subSettings.EventHandlers_.SessionClosedHandler({});
    }
    if (settings.EventHandlers_.CommonHandler_) {
        LOG_LAZY(dbDriverState->Log, TLOG_WARNING, "TSimpleBlockingWriteSession: Cannot use CommonHandler, resetting.");
        subSettings.EventHandlers_.CommonHandler({});
    }
    Writer = std::make_shared<TWriteSession>(subSettings, client, connections, dbDriverState);
    Writer->Start(TDuration::Zero());
}

uint64_t TSimpleBlockingWriteSession::GetInitSeqNo() {
    return Writer->GetInitSeqNo().GetValueSync();
}

bool TSimpleBlockingWriteSession::Write(
        std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp, const TDuration& blockTimeout
) {
    auto message = TWriteMessage(std::move(data))
        .SeqNo(seqNo)
        .CreateTimestamp(createTimestamp);
    return Write(std::move(message), nullptr, blockTimeout);
}

bool TSimpleBlockingWriteSession::Write(
        TWriteMessage&& message, TTransactionBase* tx, const TDuration& blockTimeout
) {
    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.has_value()) {
        Writer->Write(std::move(*continuationToken), std::move(message), tx);
        return true;
    }
    return false;
}

std::optional<TContinuationToken> TSimpleBlockingWriteSession::WaitForToken(const TDuration& timeout) {
    return NDetail::WaitForToken(*Writer, Closed, timeout);
}

TWriterCounters::TPtr TSimpleBlockingWriteSession::GetCounters() {
    return Writer->GetCounters();
}

bool TSimpleBlockingWriteSession::IsAlive() const {
    return !Closed.load();
}

bool TSimpleBlockingWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    return Writer->Close(std::move(closeTimeout));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingKeyedWriteSession

TSimpleBlockingKeyedWriteSession::TSimpleBlockingKeyedWriteSession(
        const TKeyedWriteSessionSettings& settings,
        std::shared_ptr<TTopicClient::TImpl> client,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        TDbDriverStatePtr dbDriverState
): Connections(connections), Client(client), DbDriverState(dbDriverState), Settings(settings) {
    TDescribeTopicSettings describeTopicSettings;
    auto topicConfig = client->DescribeTopic(settings.Path_, describeTopicSettings).GetValueSync();
    const auto& partitions = topicConfig.GetTopicDescription().GetPartitions();

    if (AutoPartitioningEnabled(topicConfig.GetTopicDescription())) {
        for (const auto& partition : partitions) {
            Partitions.emplace_back(partition.GetPartitionId(), partition.GetFromBound(), partition.GetToBound());
        }
        PartitionChooser = std::make_unique<TBoundPartitionChooser>(this);
    } else {
        for (const auto& partition : partitions) {
            Partitions.emplace_back(partition.GetPartitionId());
        }
        PartitionChooser = std::make_unique<THashPartitionChooser>(this);
    }
}

TSimpleBlockingKeyedWriteSession::WrappedWriteSessionPtr TSimpleBlockingKeyedWriteSession::CreateWriteSession(const TPartitionInfo& partitionInfo) {
    CleanExpiredSessions();

    auto alteredSettings = Settings;
    alteredSettings.DirectWriteToPartition(true);
    alteredSettings.PartitionId(partitionInfo.PartitionId);

    auto writeSession = std::make_shared<WriteSessionWrapper>(Client->CreateSimpleWriteSession(alteredSettings), partitionInfo, TInstant::Now() + Settings.SessionTimeout_);
    auto [it, _] = SessionsPool.insert(writeSession);
    SessionsIndex[partitionInfo] = it;
    return writeSession;
}

TSimpleBlockingKeyedWriteSession::WrappedWriteSessionPtr TSimpleBlockingKeyedWriteSession::GetWriteSession(const TPartitionInfo& partitionInfo) {    
    auto it = SessionsIndex.find(partitionInfo);
    if (it == SessionsIndex.end()) {
        return CreateWriteSession(partitionInfo);
    }

    if ((*it->second)->IsExpired()) {
        DestroyWriteSession(it->second, TDuration::Zero());
        return CreateWriteSession(partitionInfo);
    }

    return *it->second;
}

bool TSimpleBlockingKeyedWriteSession::Write(const std::string& key, TWriteMessage&& message, TTransactionBase* tx,
    const TDuration& blockTimeout) {
    auto partitionInfo = PartitionChooser->ChoosePartition(key);
    auto writeSession = GetWriteSession(partitionInfo);
    return writeSession->Session->Write(std::move(message), tx, blockTimeout);
}

void TSimpleBlockingKeyedWriteSession::CleanExpiredSessions() {
    ui64 cleanedSessionsCount = 0;
    for (auto it = SessionsPool.begin(); it != SessionsPool.end();) {
        if (cleanedSessionsCount >= MAX_CLEANED_SESSIONS_COUNT || !(*it)->IsExpired()) {
            break;
        }

        DestroyWriteSession(it, TDuration::Zero());
        cleanedSessionsCount++;
    }
}

bool TSimpleBlockingKeyedWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    for (auto it = SessionsPool.begin(); it != SessionsPool.end();) {
        DestroyWriteSession(it, closeTimeout);
    }
    return true;
}

void TSimpleBlockingKeyedWriteSession::DestroyWriteSession(std::set<WrappedWriteSessionPtr>::iterator& writeSession, const TDuration& closeTimeout) {
    (*writeSession)->Session->Close(closeTimeout);
    SessionsIndex.erase((*writeSession)->PartitionInfo);
    SessionsPool.erase(writeSession++);
}

TWriterCounters::TPtr TSimpleBlockingKeyedWriteSession::GetCounters() {
    return nullptr;
}

TSimpleBlockingKeyedWriteSession::TBoundPartitionChooser::TBoundPartitionChooser(TSimpleBlockingKeyedWriteSession* session) : Session(session) {}

const TSimpleBlockingKeyedWriteSession::TPartitionInfo& TSimpleBlockingKeyedWriteSession::TBoundPartitionChooser::ChoosePartition(const std::string& key) {
    auto it = std::find_if(Session->Partitions.begin(), Session->Partitions.end(), [key](const auto& partitionInfo) {
        return partitionInfo.InRange(key);
    });
    Y_ABORT_UNLESS(it != Session->Partitions.end());
    return *it;
}

TSimpleBlockingKeyedWriteSession::THashPartitionChooser::THashPartitionChooser(TSimpleBlockingKeyedWriteSession* session) : Session(session) {}

const TSimpleBlockingKeyedWriteSession::TPartitionInfo& TSimpleBlockingKeyedWriteSession::THashPartitionChooser::ChoosePartition(const std::string& key) {
    return Session->Partitions[std::hash<std::string>{}(key) % Session->Partitions.size()];
}

} // namespace NYdb::NTopic
