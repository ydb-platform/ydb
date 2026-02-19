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

    // Локальное «окно» для лимитирования скорости. Глобальные bytesWritten/startTimestamp
    // используются только для генерации expectedTs/логов, а именно здесь мы ограничиваем
    // текущую скорость. Это позволяет не тянуть за собой «долг» по скорости на весь
    // ран ворклоада и избежать долговременного проседания MB/s после разовых всплесков.
    TInstant rateWindowStart = startTimestamp;
    ui64 bytesWrittenInWindow = 0;

    // Диагностические счётчики по текущему окну, чтобы понимать,
    // чем именно ограничивается скорость записи.
    ui64 windowTokenLimitedIterations = 0;   // сколько раз не было continuation token
    ui64 windowRateLimitedIterations = 0;    // сколько раз упёрлись в BytesPerSec

    TInstant lastWaitTime = TInstant::Zero();
    while (!*params.ErrorFlag) {
        const auto now = Now();
        if (now > endTime) {
            break;
        }

        auto producer = producers[partitionToWriteId % producers.size()];

        TDuration timeToNextMessage = TDuration::Zero();
        if (params.BytesPerSec != 0) {
            // Локальное ожидаемое время следующего сообщения в рамках текущего окна.
            const auto expectedTsInWindow = rateWindowStart + TDuration::Seconds(
                (double)bytesWrittenInWindow / params.BytesPerSec * params.ProducerThreadCount);
            timeToNextMessage = expectedTsInWindow - now;
        }

        const bool hasTokenBeforeWait = hasContinuationToken(producer);
        if (timeToNextMessage > TDuration::Zero() || !hasTokenBeforeWait) {
            if (!hasTokenBeforeWait) {
                ++windowTokenLimitedIterations;
            }
            producer->WaitForContinuationToken(timeToNextMessage);
        }

        bool writingAllowed = hasContinuationToken(producer);
        if (!writingAllowed) {
            lastWaitTime = TInstant::Now();
            WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, LogPrefix(sessionId) << "No continuation token found, will wait for 1ms");
        } else if (lastWaitTime != TInstant::Zero()) {
            WRITE_LOG(params.Log, ELogPriority::TLOG_ERR, LogPrefix(sessionId) << "Continuation token found, waited for " << (TInstant::Now() - lastWaitTime).MilliSeconds() << "ms");
            lastWaitTime = TInstant::Zero();
        }

        TDuration toWait = TDuration::Zero();

        if (params.BytesPerSec != 0) {
            // How many bytes had to be written in the current window by this particular producer.
            const ui64 bytesMustBeWritten = (now - rateWindowStart).SecondsFloat()
                * params.BytesPerSec / params.ProducerThreadCount;
            const bool rateLimitedNow = !(bytesWrittenInWindow < bytesMustBeWritten);
            if (rateLimitedNow) {
                ++windowRateLimitedIterations;
            }
            writingAllowed &= !rateLimitedNow;
            if (bytesWrittenInWindow > bytesMustBeWritten) {
                toWait = TDuration::Seconds(
                    (double)(bytesWrittenInWindow - bytesMustBeWritten)
                    / params.BytesPerSec * params.ProducerThreadCount);
                // Cap so we don't sleep past idle timeout (lastActivityTime is not updated in this branch).
                if (toWait >= idleTimeout) {
                    toWait = idleTimeout - TDuration::MilliSeconds(100);
                }
            }
        } else {
            writingAllowed &= inflightMessagesSize() <= 1_MB / params.MessageSize;
        }

        if (writingAllowed && !waitForCommitTx) {
            const TInstant createTimestamp = getCreateTs();
            // Глобальный счётчик для expectedTs/логов.
            bytesWritten += params.MessageSize;
            // Локальный счётчик в рамках текущего окна лимита скорости.
            bytesWrittenInWindow += params.MessageSize;

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
        } else {
            if (txSupport) {
                tryCommitTx(commitTime);
            }
            // When toWait is 0 (e.g. waiting for token), sleep briefly to avoid busy loop.
            Sleep(toWait > TDuration::Zero() ? toWait : TDuration::MilliSeconds(1));
        }

        // Переодически «перезапускаем» окно лимита скорости, чтобы не накапливать
        // историю overshoot/undershoot на весь срок работы ворклоада.
        if (now - rateWindowStart >= TDuration::Seconds(1)) {
            // Для диагностики выводим статистику по оконному лимиту и токенам.
            if (params.BytesPerSec != 0) {
                const double windowSeconds = (now - rateWindowStart).SecondsFloat();
                const ui64 bytesMustBeWrittenWindow = windowSeconds > 0
                    ? static_cast<ui64>(windowSeconds * params.BytesPerSec / params.ProducerThreadCount)
                    : 0;
                WRITE_LOG(params.Log, ELogPriority::TLOG_ERR,
                    LogPrefix(sessionId)
                        << "Rate window stats: bytesWrittenInWindow=" << bytesWrittenInWindow
                        << " bytesMustBeWrittenWindow=" << bytesMustBeWrittenWindow
                        << " tokenLimitedIters=" << windowTokenLimitedIterations
                        << " rateLimitedIters=" << windowRateLimitedIterations);
            }

            rateWindowStart = now;
            bytesWrittenInWindow = 0;
            windowTokenLimitedIterations = 0;
            windowRateLimitedIterations = 0;
        }
    }
}

} // namespace NYdb::NConsoleClient::NTopicWorkloadWriterInternal

