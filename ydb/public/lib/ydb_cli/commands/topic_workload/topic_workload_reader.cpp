#include "topic_workload_reader.h"
#include "topic_workload_reader_transaction_support.h"

#include "topic_workload_describe.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

using namespace NYdb::NConsoleClient;

void TTopicWorkloadReader::RetryableReaderLoop(TTopicWorkloadReaderParams& params) {
    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*params.ErrorFlag && Now() < endTime) {
        try {
            ReaderLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadReader::ReaderLoop(TTopicWorkloadReaderParams& params, TInstant endTime) {
    auto topicClient = std::make_unique<NYdb::NTopic::TTopicClient>(params.Driver);
    std::optional<TTransactionSupport> txSupport;

    auto describeTopicResult = TCommandWorkloadTopicDescribe::DescribeTopic(params.Database, params.TopicName, params.Driver);
    NYdb::NTopic::TReadSessionSettings settings;
    settings.AutoPartitioningSupport(true);

    if (!params.ReadWithoutConsumer) {
        auto consumerName = TCommandWorkloadTopicDescribe::GenerateConsumerName(params.ConsumerPrefix, params.ConsumerIdx);
        auto consumers = describeTopicResult.GetConsumers();

        if (!std::any_of(consumers.begin(), consumers.end(), [consumerName](const auto& consumer) { return consumer.GetConsumerName() == consumerName; }))
        {
            WRITE_LOG(params.Log, ELogPriority::TLOG_EMERG, TStringBuilder() << "Topic '" << params.TopicName << "' doesn't have a consumer '" << consumerName << "'. Run command 'workload init' with parameter '--consumers'.");
            exit(EXIT_FAILURE);
        }
        settings.ConsumerName(consumerName).AppendTopics(params.TopicName);
    } else {
        NYdb::NTopic::TTopicReadSettings topic = params.TopicName;
        auto partitions = describeTopicResult.GetPartitions();
        for(auto partition: partitions) {
            topic.AppendPartitionIds(partition.GetPartitionId());
        }
        settings.WithoutConsumer().AppendTopics(topic);
    }


    if (params.UseTransactions) {
        txSupport.emplace(params.Driver, params.ReadOnlyTableName, params.TableName);
    }

    auto readSession = topicClient->CreateReadSession(settings);
    WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, "Reader session was created.");

    struct TPartitionStreamState {
        ui64 StartOffset;
        NYdb::NTopic::TPartitionSession::TPtr Stream;
    };
    THashMap<std::pair<TString, ui64>, TPartitionStreamState> streamState;

    TInstant LastPartitionStatusRequestTime = TInstant::Zero();

    (*params.StartedCount)++;

    TInstant commitTime = Now() + TDuration::Seconds(params.CommitPeriod);

    TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent> stopPartitionSessionEvents;

    while (Now() < endTime && !*params.ErrorFlag) {
        auto now = TInstant::Now();
        if (now - LastPartitionStatusRequestTime > TDuration::Seconds(1)) {
            for (auto& st : streamState) {
                if (st.second.Stream) {
                    st.second.Stream->RequestStatus();
                }
            }
            LastPartitionStatusRequestTime = now;
        }

        readSession->WaitEvent().Wait(TDuration::Seconds(1));
        TVector<NYdb::NTopic::TReadSessionEvent::TEvent> events = GetEvents(*readSession, params, txSupport);

        // we could wait for the event for almost one second, so we need to update the value of the variable
        now = TInstant::Now();

        for (auto& event : events) {
            if (auto* dataEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                WRITE_LOG(params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << dataEvent->DebugString());

                for (const auto& message : dataEvent->GetMessages()) {
                    ui64 fullTime = (now - message.GetCreateTime()).MilliSeconds();
                    params.StatsCollector->AddReaderEvent(params.ReaderIdx, {message.GetData().Size(), fullTime});

                    if (txSupport) {
                        txSupport->AppendRow(message.GetData());
                    }

                    WRITE_LOG(params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Got message: " << message.GetMessageGroupId()
                        << " topic " << message.GetPartitionSession()->GetTopicPath() << " partition " << message.GetPartitionSession()->GetPartitionId()
                        << " offset " << message.GetOffset() << " seqNo " << message.GetSeqNo()
                        << " createTime " << message.GetCreateTime() << " fullTimeMs " << fullTime);
                }

                if (!params.ReadWithoutConsumer && (!txSupport || params.UseTopicCommit)) {
                    dataEvent->Commit();
                }
            } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                auto stream = createPartitionStreamEvent->GetPartitionSession();
                ui64 startOffset = streamState[std::make_pair(stream->GetTopicPath(), stream->GetPartitionId())].StartOffset;
                streamState[std::make_pair(stream->GetTopicPath(), stream->GetPartitionId())].Stream = stream;
                WRITE_LOG(params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Starting read " << createPartitionStreamEvent->DebugString() << " from " << startOffset);
                createPartitionStreamEvent->Confirm();
            } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                auto stream = destroyPartitionStreamEvent->GetPartitionSession();
                streamState[std::make_pair(stream->GetTopicPath(), stream->GetPartitionId())].Stream = nullptr;

                if (txSupport) {
                    // gracefull shutdown. we will send confirmations later
                    stopPartitionSessionEvents.push_back(std::move(*destroyPartitionStreamEvent));
                } else {
                    destroyPartitionStreamEvent->Confirm();
                }
            } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, TStringBuilder() << "Read session closed: " << closeSessionEvent->DebugString());
                *params.ErrorFlag = 1;
                break;
            } else if (auto* endPartitionStreamEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                endPartitionStreamEvent->Confirm();
            } else if (auto* partitionStreamStatusEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
                WRITE_LOG(params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << partitionStreamStatusEvent->DebugString())

                ui64 lagMessages = partitionStreamStatusEvent->GetEndOffset() - partitionStreamStatusEvent->GetCommittedOffset();
                ui64 lagTime = lagMessages == 0 ? 0 : (now - partitionStreamStatusEvent->GetWriteTimeHighWatermark()).MilliSeconds();

                params.StatsCollector->AddLagEvent(params.ReaderIdx, {lagMessages, lagTime});
            } else if (auto* ackEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
                WRITE_LOG(params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << ackEvent->DebugString());
            }
        }

        if (txSupport) {
            TryCommitTx(params, txSupport, commitTime, stopPartitionSessionEvents);
        }
    }
}

TVector<NYdb::NTopic::TReadSessionEvent::TEvent> TTopicWorkloadReader::GetEvents(NYdb::NTopic::IReadSession& readSession,
                                                                                 TTopicWorkloadReaderParams& params,
                                                                                 std::optional<TTransactionSupport>& txSupport)
{
    TVector<NYdb::NTopic::TReadSessionEvent::TEvent> events;
    NTopic::TReadSessionGetEventSettings settings;

    settings.Block(false);

    if (txSupport) {
        if (!txSupport->Transaction) {
            txSupport->BeginTx();
        }

        if (!params.UseTopicCommit) {
            settings.Tx(*txSupport->Transaction);
        }
    }

    return readSession.GetEvents(settings);
}

void TTopicWorkloadReader::TryCommitTx(TTopicWorkloadReaderParams& params,
                                       std::optional<TTransactionSupport>& txSupport,
                                       TInstant& commitTime,
                                       TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents)
{
    Y_ABORT_UNLESS(txSupport);

    if ((commitTime > Now()) && (params.CommitMessages > txSupport->Rows.size())) {
        return;
    }

    TryCommitTableChanges(params, txSupport);
    GracefullShutdown(stopPartitionSessionEvents);

    commitTime += TDuration::Seconds(params.CommitPeriod);
}

void TTopicWorkloadReader::TryCommitTableChanges(TTopicWorkloadReaderParams& params,
                                                 std::optional<TTransactionSupport>& txSupport)
{
    if (txSupport->Rows.empty()) {
        return;
    }

    auto execTimes = txSupport->CommitTx(params.UseTableSelect, params.UseTableUpsert);

    params.StatsCollector->AddSelectEvent(params.ReaderIdx, {execTimes.SelectTime.MilliSeconds()});
    params.StatsCollector->AddUpsertEvent(params.ReaderIdx, {execTimes.UpsertTime.MilliSeconds()});
    params.StatsCollector->AddCommitTxEvent(params.ReaderIdx, {execTimes.CommitTime.MilliSeconds()});
}

void TTopicWorkloadReader::GracefullShutdown(TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents)
{
    for (auto& event : stopPartitionSessionEvents) {
        event.Confirm();
    }
    stopPartitionSessionEvents.clear();
}
