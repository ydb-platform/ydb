#include "sqs_workload_stats.h"

using namespace NYdb::NConsoleClient;

TSqsWorkloadStats::TSqsWorkloadStats()
    : WriteBytes(0)
    , WriteMessages(0)
    , SendRequestTimeHist(HighestTrackableTime, 2)
    , ReadMessages(0)
    , SendRequestErrors(0)
    , ReceiveRequestTimeHist(HighestTrackableTime, 2)
    , ReceiveRequestErrors(0)
    , DeleteMessages(0)
    , DeleteRequestTimeHist(HighestTrackableTime, 2)
    , DeleteRequestErrors(0)
    , ReadBytes(0)
    , EndToEndLatencyHist(HighestTrackableTime, 5)
    , MessagesInFlight(0)
    , MessagesInFlightHist(HighestTrackableMessageCount, 5)
{
}

void TSqsWorkloadStats::AddEvent(const SendRequestDoneEvent& event)
{
    SendRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const SentMessagesEvent& event)
{
    WriteBytes += event.TotalBytes;
    WriteMessages += event.MessagesCount;
}

void TSqsWorkloadStats::AddEvent(const ReceiveRequestDoneEvent& event)
{
    ReceiveRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const DeleteRequestDoneEvent& event)
{
    DeleteRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const DeletedMessagesEvent& event)
{
    DeleteMessages += event.MessagesCount;
    if (MessagesInFlight > event.MessagesCount) {
        MessagesInFlight -= event.MessagesCount;
    } else {
        MessagesInFlight = 0;
    }
    MessagesInFlightHist.RecordValue(MessagesInFlight);
}

void TSqsWorkloadStats::AddEvent(const GotMessageEvent& event)
{
    ReadMessages += event.BatchSize;
    ReadBytes += event.MessagesSize;
    EndToEndLatencyHist.RecordValue(Min(event.EndToEndLatency, HighestTrackableTime));
    MessagesInFlight += event.BatchSize;
    MessagesInFlightHist.RecordValue(MessagesInFlight);
}

void TSqsWorkloadStats::AddEvent(const SendRequestErrorEvent&)
{
    SendRequestErrors++;
}

void TSqsWorkloadStats::AddEvent(const ReceiveRequestErrorEvent&)
{
    ReceiveRequestErrors++;
}

void TSqsWorkloadStats::AddEvent(const DeleteRequestErrorEvent&)
{
    DeleteRequestErrors++;
}