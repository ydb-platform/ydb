#include "topic_workload_writer.h"
#include "topic_workload_writer_producer.h"
#include "topic_workload_describe.h"

#include <util/generic/overloaded.h>
#include <util/generic/guid.h>

using namespace NYdb::NConsoleClient;

TTopicWorkloadWriterWorker::TTopicWorkloadWriterWorker(
    TTopicWorkloadWriterParams&& params)
    : Params(std::move(params))
    , StatsCollector(Params.StatsCollector)

{
    Producers = std::vector<TTopicWorkloadWriterProducer>();
    Producers.reserve(Params.ProducersPerThread);
    for (ui32 i = 0; i < Params.ProducersPerThread; ++i) {
        // write to random partition, cause workload CLI tool can be launched in several instances
        // and they need to load different partitions of the topic
        ui32 partitionId = (Params.PartitionSeed + i) % Params.PartitionCount;

        Producers.emplace_back(
                Params,
                StatsCollector,
                TGUID::CreateTimebased().AsGuidString(), // ProducerId
                partitionId
        );
    }

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "WriterId " << Params.WriterIdx
            << ": Initialized " << Params.ProducersPerThread << " producers");
}

TTopicWorkloadWriterWorker::~TTopicWorkloadWriterWorker()
{
    for (auto &producer: Producers) {
        producer.Close();
    }
}

void TTopicWorkloadWriterWorker::Close()
{
    Closed->store(true);
    for (auto &producer: Producers) {
        producer.Close();
    }
}

std::vector<TString> TTopicWorkloadWriterWorker::GenerateMessages(size_t messageSize) {
    std::vector<TString> generatedMessages;
    for (size_t i = 0; i < GENERATED_MESSAGES_COUNT; i++) {
        TStringBuilder stringBuilder;
        while (stringBuilder.size() < messageSize)
            stringBuilder << RandomNumber<ui64>(UINT64_MAX);
        stringBuilder.resize(messageSize);
        generatedMessages.push_back(std::move(stringBuilder));
    }
    return generatedMessages;
}

/*!
 * This method returns timestamp, when current message is expected to be generated.
 *
 * E.g. if we have 100 messages per second rate, then first message is expected at 0ms of first second, second after 10ms elapsed,
 * third after 20ms elapsed, 10th after 100ms elapsed and so on. This way 101 message is expected to be generated not earlier
 * then 1 second and 10 ms after test start has passed.
 * */
TInstant TTopicWorkloadWriterWorker::GetExpectedCurrMessageCreationTimestamp() const {
    return StartTimestamp + TDuration::Seconds((double) BytesWritten / Params.BytesPerSec * Params.ProducerThreadCount);
}

void TTopicWorkloadWriterWorker::WaitTillNextMessageExpectedCreateTimeAndContinuationToken(TTopicWorkloadWriterProducer& producer) {
    auto now = Now();
    TDuration timeToNextMessage = Params.BytesPerSec == 0 ? TDuration::Zero() :
                                  GetExpectedCurrMessageCreationTimestamp() - now;

    if (timeToNextMessage > TDuration::Zero() || !producer.ContinuationToken)
    {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "WriterId " << Params.WriterIdx
            << " producer id " << producer.ProducerId
            << " for partition " << producer.PartitionId
            << ": WaitEvent for timeToNextMessage " << timeToNextMessage);
        producer.WaitForContinuationToken(timeToNextMessage);
    } else
    {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WriterIdx: "
            << Params.WriterIdx << " for partition " << producer.PartitionId <<  ": No wait");
    }
}

void TTopicWorkloadWriterWorker::Process(TInstant endTime) {
    Sleep(TDuration::Seconds((float) Params.WarmupSec * Params.WriterIdx / Params.ProducerThreadCount));

    TInstant commitTime = TInstant::Now() + TDuration::MilliSeconds(Params.CommitPeriodMs);

    StartTimestamp = Now();
    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "WriterIdx: "
                                                                    << Params.WriterIdx
                                                                    << ": StartTimestamp " << StartTimestamp);

    while (!*Params.ErrorFlag)
    {
        auto now = Now();
        if (now > endTime)
            break;

        auto& producer = Producers[ProducerIndex % Params.ProducersPerThread];

        WaitTillNextMessageExpectedCreateTimeAndContinuationToken(producer);

        bool writingAllowed = producer.ContinuationToken.Defined();

        if (Params.BytesPerSec != 0)
        {
            // how many bytes had to be written till this second by this particular producer
            ui64 bytesMustBeWritten = (now - StartTimestamp).SecondsFloat() * Params.BytesPerSec / Params.ProducerThreadCount;
            writingAllowed &= BytesWritten < bytesMustBeWritten;
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "BytesWritten " << BytesWritten << " bytesMustBeWritten " << bytesMustBeWritten << " writingAllowed " << writingAllowed);
        }
        else
        {
            writingAllowed &= InflightMessagesSize() <= 1_MB / Params.MessageSize;
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder() << "Inflight size " << InflightMessagesSize() << " writingAllowed " << writingAllowed);
        }

        if (writingAllowed && !WaitForCommitTx)
        {
            TInstant createTimestamp = Params.BytesPerSec == 0 ? now : GetExpectedCurrMessageCreationTimestamp();
            BytesWritten += Params.MessageSize;

            std::optional<NYdb::NTable::TTransaction> transaction = {};
            if (TxSupport && !TxSupport->Transaction) {
                TxSupport->BeginTx();
            }
            if (TxSupport) {
                transaction = *TxSupport->Transaction;
            }

            producer.Send(createTimestamp, transaction);

            if (TxSupport) {
                TxSupport->AppendRow("");
                TryCommitTx(commitTime);
            }

            // ToDo: put in callback
            WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
                    << "Written message " << producer.MessageId - 1
                    << " in writer " << Params.WriterIdx
                    << " For partition " << producer.PartitionId
                    << " message create ts " << createTimestamp
                    << " delta from now " << (Params.BytesPerSec == 0 ? TDuration() : now - createTimestamp));
        }
        else
        {
            if (TxSupport) {
                TryCommitTx(commitTime);
            }

            Sleep(TDuration::MilliSeconds(1));
        }

        ProducerIndex++;
    }
}

size_t TTopicWorkloadWriterWorker::InflightMessagesSize() {
    size_t total = 0;

    for (auto &producer: Producers) {
        total += producer.InflightMessagesCreateTs.size();
    }

    return total;
}

bool TTopicWorkloadWriterWorker::InflightMessagesEmpty() {
    for (auto &producer: Producers) {
        if (!producer.InflightMessagesCreateTs.empty()) {
            return false;
        }
    }

    return true;
}

void TTopicWorkloadWriterWorker::RetryableWriterLoop(TTopicWorkloadWriterParams& params) {
    auto logger = params.Log;
    auto errorFlag = params.ErrorFlag;

    const TInstant endTime = Now() + TDuration::Seconds(params.TotalSec + 3);

    while (!*errorFlag && Now() < endTime) {
        try {
            WriterLoop(params, endTime);
        } catch (const yexception& ex) {
            WRITE_LOG(logger, ELogPriority::TLOG_WARNING, TStringBuilder() << ex);
        }
    }
}

void TTopicWorkloadWriterWorker::WriterLoop(TTopicWorkloadWriterParams& params, TInstant endTime) {
    TTopicWorkloadWriterWorker writer(std::move(params));

    if (params.UseTransactions) {
        writer.TxSupport.emplace(params.Driver, "", "");
    }

    (*writer.Params.StartedCount)++;

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer started " << Now().ToStringUpToSeconds());

    for (auto& producer : writer.Producers) {
        if (!producer.WaitForInitSeqNo())
            return;
    }

    try {
        writer.Process(endTime);
    } catch (const std::runtime_error& re) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Writer " << writer.Params.WriterIdx << " failed with error: " << re.what());
    } catch (...) {
        WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_ERR, TStringBuilder()
            << "Writer " << writer.Params.WriterIdx << " caught unknown exception: " << CurrentExceptionMessage());
    }

    WRITE_LOG(writer.Params.Log, ELogPriority::TLOG_INFO, TStringBuilder() << "Writer finished " << Now().ToStringUpToSeconds());
}

void TTopicWorkloadWriterWorker::TryCommitTx(TInstant& commitTime)
{
    Y_ABORT_UNLESS(TxSupport);
    auto now = Now();

    bool commitTimeIsInFuture = now < commitTime;
    bool notEnoughRowsInCommit = TxSupport->Rows.size() < Params.CommitMessages;
    if (commitTimeIsInFuture && notEnoughRowsInCommit) {
        WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()
            << "Not committing: "
            << "commit time: " << commitTime
            << " now: " << now
            << ", messages needed for commit: " << Params.CommitMessages
            << " current rows in transactions: " << TxSupport->Rows.size()
        );
        return;
    }

    if (!InflightMessagesEmpty()) {
        WaitForCommitTx = true;
        return;
    }

    TryCommitTableChanges();

    commitTime += TDuration::MilliSeconds(Params.CommitPeriodMs);

    WaitForCommitTx = false;
}

void TTopicWorkloadWriterWorker::TryCommitTableChanges()
{
    if (TxSupport->Rows.empty()) {
        return;
    }

    WRITE_LOG(Params.Log, ELogPriority::TLOG_DEBUG, TStringBuilder()<< "Starting commit");

    auto execTimes = TxSupport->CommitTx(Params.UseTableSelect, Params.UseTableUpsert);

    Params.StatsCollector->AddWriterSelectEvent(Params.WriterIdx, {execTimes.SelectTime.MilliSeconds()});
    Params.StatsCollector->AddWriterUpsertEvent(Params.WriterIdx, {execTimes.UpsertTime.MilliSeconds()});
    Params.StatsCollector->AddWriterCommitTxEvent(Params.WriterIdx, {execTimes.CommitTime.MilliSeconds()});
}
