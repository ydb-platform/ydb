#include "sqs_workload_stats.h"

using namespace NYdb::NConsoleClient;

TSqsWorkloadStats::TSqsWorkloadStats()
    : WriteBytes(0),
    WriteMessages(0),
    SendRequestTimeHist(HighestTrackableTime, 2),
    ReadMessages(0),
    SendRequestErrors(0),
    SendRequestsSuccess(0),
    ReceiveRequestTimeHist(HighestTrackableTime, 2),
    ReceiveRequestErrors(0),
    ReceiveRequestsSuccess(0),
    DeleteMessages(0),
    DeleteRequestTimeHist(HighestTrackableTime, 2),
    DeleteRequestErrors(0),
    DeleteRequestsSuccess(0),
    ReadBytes(0),
    EndToEndLatencyHist(HighestTrackableTime, 5),
    MessagesInFlight(0),
    MessagesInFlightHist(HighestTrackableMessageCount, 5),
    AsyncRequestTasks(0),
    ErrorsWhileProcessingMessages(0)
{
}

void TSqsWorkloadStats::AddEvent(const ErrorWhileProcessingMessagesEvent&)
{
    ErrorsWhileProcessingMessages++;
}

void TSqsWorkloadStats::AddEvent(const SendRequestDoneEvent& event)
{
    if (event.RequestTime > 0) {
        SendRequestTimeHist.RecordValue(
            Min(event.RequestTime, HighestTrackableTime));
    }
}

void TSqsWorkloadStats::AddEvent(const SentMessagesEvent& event)
{
    WriteBytes += event.TotalBytes;
    WriteMessages += event.MessagesCount;
    AsyncRequestTasks--;
    SendRequestsSuccess += event.MessagesCount;
}

void TSqsWorkloadStats::AddEvent(const ReceiveRequestDoneEvent& event)
{
    if (event.RequestTime > 0) {
        ReceiveRequestTimeHist.RecordValue(
            Min(event.RequestTime, HighestTrackableTime));
    }
}

void TSqsWorkloadStats::AddEvent(const DeleteRequestDoneEvent& event)
{
    if (event.RequestTime > 0) {
        DeleteRequestTimeHist.RecordValue(
            Min(event.RequestTime, HighestTrackableTime));
    }
}

void TSqsWorkloadStats::AddEvent(const DeletedMessagesEvent& event)
{
    DeleteMessages += event.MessagesCount;
    DeleteRequestsSuccess += event.MessagesCount;
}

void TSqsWorkloadStats::AddEvent(const GotMessageEvent& event)
{
    ReadMessages += event.BatchSize;
    ReadBytes += event.MessagesSize;
    ReceiveRequestsSuccess += event.BatchSize;
    EndToEndLatencyHist.RecordValue(
        Min(event.EndToEndLatency, HighestTrackableTime));
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
    AsyncRequestTasks--;
}

void TSqsWorkloadStats::AddEvent(const DeleteRequestErrorEvent&)
{
    DeleteRequestErrors++;
}

void TSqsWorkloadStats::AddEvent(const FinishProcessMessagesEvent& event)
{
    if (MessagesInFlight > event.MessagesCount) {
        MessagesInFlight -= event.MessagesCount;
    } else {
        MessagesInFlight = 0;
    }
    MessagesInFlightHist.RecordValue(MessagesInFlight);
    AsyncRequestTasks--;
}

void TSqsWorkloadStats::AddEvent(const PushAsyncRequestTaskToQueueEvent&)
{
    AsyncRequestTasks++;
}
