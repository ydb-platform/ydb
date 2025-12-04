#include "sqs_workload_stats.h"

using namespace NYdb::NConsoleClient;

TSqsWorkloadStats::TSqsWorkloadStats()
    : WriteBytes(0)
    , WriteMessages(0)
    , SendRequestTimeHist(HighestTrackableTime, 2)
    , ReadMessages(0)
    , ReceiveRequestTimeHist(HighestTrackableTime, 2)
    , DeleteMessages(0)
    , DeleteRequestTimeHist(HighestTrackableTime, 2)
    , ReadBytes(0)
    , EndToEndLatencyHist(HighestTrackableTime, 5)
{
}

void TSqsWorkloadStats::AddEvent(const SendRequestDoneEvent& event)
{
    WriteMessages += event.MessageCount;
    WriteBytes += event.TotalBytes;
    SendRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const ReceiveRequestDoneEvent& event)
{
    ReceiveRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const DeleteRequestDoneEvent& event)
{
    DeleteMessages += event.MessageCount;
    DeleteRequestTimeHist.RecordValue(Min(event.RequestTime, HighestTrackableTime));
}

void TSqsWorkloadStats::AddEvent(const GotMessageEvent& event)
{
    ReadMessages++;
    ReadBytes += event.MessageSize;
    EndToEndLatencyHist.RecordValue(Min(event.EndToEndLatency, HighestTrackableTime));
}

