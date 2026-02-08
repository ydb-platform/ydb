#pragma once

#include "topic_workload_writer.h"

#include <util/system/datetime.h>
#include <util/system/yassert.h>

#include <vector>

namespace NYdb::NConsoleClient::NTopicWorkloadWriterInternal {

inline TInstant GetExpectedCurrMessageCreationTimestamp(
    const TInstant& startTimestamp,
    ui64 bytesWritten,
    const TTopicWorkloadWriterParams& params
) {
    // Caller is expected to use this only when BytesPerSec != 0.
    Y_ABORT_UNLESS(params.BytesPerSec != 0);
    return startTimestamp + TDuration::Seconds((double)bytesWritten / params.BytesPerSec * params.ProducerThreadCount);
}

inline TInstant GetCreateTimestampForNextMessage(const TTopicWorkloadWriterParams& params, const TInstant& expectedTs) {
    if (params.UseCpuTimestamp || params.BytesPerSec == 0) {
        return TInstant::Now();
    }
    return expectedTs;
}

inline void CommitTableChangesCommon(
    const TTopicWorkloadWriterParams& params,
    std::optional<TTransactionSupport>& txSupport
) {
    Y_ABORT_UNLESS(txSupport);
    if (txSupport->Rows.empty()) {
        return;
    }

    const auto execTimes = txSupport->CommitTx(params.UseTableSelect, params.UseTableUpsert);
    params.StatsCollector->AddWriterSelectEvent(params.WriterIdx, {execTimes.SelectTime.MilliSeconds()});
    params.StatsCollector->AddWriterUpsertEvent(params.WriterIdx, {execTimes.UpsertTime.MilliSeconds()});
    params.StatsCollector->AddWriterCommitTxEvent(params.WriterIdx, {execTimes.CommitTime.MilliSeconds()});
}

template <class TOnNotCommitting>
inline void TryCommitTxCommon(
    const TTopicWorkloadWriterParams& params,
    std::optional<TTransactionSupport>& txSupport,
    TInstant& commitTime,
    bool& waitForCommitTx,
    TOnNotCommitting onNotCommitting
) {
    Y_ABORT_UNLESS(txSupport);
    const auto now = Now();

    const bool commitTimeIsInFuture = now < commitTime;
    const bool notEnoughRowsInCommit = txSupport->Rows.size() < params.CommitMessages;
    if (commitTimeIsInFuture && notEnoughRowsInCommit) {
        onNotCommitting(now, commitTime, txSupport->Rows.size());
        return;
    }

    CommitTableChangesCommon(params, txSupport);

    commitTime += TDuration::MilliSeconds(params.CommitIntervalMs);
    waitForCommitTx = false;
}

template <class TProducer>
inline size_t InflightMessagesSize(const std::vector<std::shared_ptr<TProducer>>& producers) {
    size_t total = 0;
    for (const auto& producer : producers) {
        total += producer->InflightMessagesCnt();
    }
    return total;
}

template <class TProducer>
inline bool InflightMessagesEmpty(const std::vector<std::shared_ptr<TProducer>>& producers) {
    for (const auto& producer : producers) {
        if (producer->InflightMessagesCnt() != 0) {
            return false;
        }
    }
    return true;
}

inline TStringBuilder LogPrefix(const std::string& sessionId) {
    return TStringBuilder() << " SessionId: " << sessionId << " ";
}

template <
    class TProducer,
    class THasContinuationToken,
    class TGetExpectedTs,
    class TGetCreateTs,
    class TTryCommitTx,
    class TInflightSize,
    class TOnStart,
    class TOnAfterSend
>
inline void ProcessWriterLoopCommon(
    const TTopicWorkloadWriterParams& params,
    std::vector<std::shared_ptr<TProducer>>& producers,
    ui64& bytesWritten,
    ui64& partitionToWriteId,
    TInstant& startTimestamp,
    std::optional<TTransactionSupport>& txSupport,
    bool& waitForCommitTx,
    TInstant endTime,
    THasContinuationToken hasContinuationToken,
    TGetExpectedTs getExpectedTs,
    TGetCreateTs getCreateTs,
    TTryCommitTx tryCommitTx,
    TInflightSize inflightMessagesSize,
    TOnStart onStart,
    TOnAfterSend onAfterSend,
    std::string sessionId,
    TDuration idleTimeout = TDuration::Max()
) {
    Y_ABORT_UNLESS(!producers.empty());

    Sleep(TDuration::Seconds((float)params.WarmupSec * params.WriterIdx / params.ProducerThreadCount));

    TInstant commitTime = TInstant::Now() + TDuration::MilliSeconds(params.CommitIntervalMs);

    startTimestamp = Now();
    onStart(startTimestamp);

    TInstant lastWaitTime = TInstant::Zero();
    TInstant lastActivityTime = TInstant::Now();
    while (!*params.ErrorFlag) {
        const auto now = Now();
        if (now > endTime) {
            break;
        }

        auto producer = producers[partitionToWriteId % producers.size()];

        const TDuration timeToNextMessage = params.BytesPerSec == 0
            ? TDuration::Zero()
            : (getExpectedTs() - now);

        if (timeToNextMessage > TDuration::Zero() || !hasContinuationToken(producer)) {
            producer->WaitForContinuationToken(timeToNextMessage);
        }

        bool writingAllowed = hasContinuationToken(producer);
        if (!writingAllowed) {
            lastWaitTime = TInstant::Now();
            WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, LogPrefix(sessionId) << "No continuation token found, will wait for 1ms");
        } else if (lastWaitTime != TInstant::Zero()) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_INFO, LogPrefix(sessionId) << "Continuation token found, waited for " << (TInstant::Now() - lastWaitTime).MilliSeconds() << "ms");
            lastWaitTime = TInstant::Zero();
        }

        if (params.BytesPerSec != 0) {
            // How many bytes had to be written till this moment by this particular producer.
            const ui64 bytesMustBeWritten = (now - startTimestamp).SecondsFloat()
                * params.BytesPerSec / params.ProducerThreadCount;
            writingAllowed &= bytesWritten < bytesMustBeWritten;
        } else {
            writingAllowed &= inflightMessagesSize() <= 1_MB / params.MessageSize;
        }

        if (writingAllowed && !waitForCommitTx) {
            const TInstant createTimestamp = getCreateTs();
            bytesWritten += params.MessageSize;

            std::optional<NYdb::NTable::TTransaction> transaction;
            if (txSupport && !txSupport->Transaction) {
                txSupport->BeginTx();
            }
            if (txSupport) {
                transaction = *txSupport->Transaction;
            }

            producer->Send(createTimestamp, transaction);

            if (txSupport) {
                txSupport->AppendRow("");
                tryCommitTx(commitTime);
            }

            onAfterSend(producer, createTimestamp, now);

            ++partitionToWriteId;
            lastActivityTime = TInstant::Now();
        } else {
            if (txSupport) {
                tryCommitTx(commitTime);
            }
            Sleep(TDuration::MilliSeconds(1));
            Y_ABORT_UNLESS(TInstant::Now() - lastActivityTime < idleTimeout, "Idle timeout reached");
        }
    }
}

} // namespace NYdb::NConsoleClient::NTopicWorkloadWriterInternal

