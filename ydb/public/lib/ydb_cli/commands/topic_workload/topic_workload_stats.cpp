#include "topic_workload_stats.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStats::TTopicWorkloadStats()
    : WriteBytes(0)
    , WriteMessages(0)
    , WriteTimeHist(HighestTrackableTime, 2)
    , InflightMessagesHist(HighestTrackableMessageCount, 2)
    , LagMessagesHist(HighestTrackableMessageCount, 2)
    , LagTimeHist(HighestTrackableTime, 2)
    , ReadBytes(0)
    , ReadMessages(0)
    , FullTimeHist(HighestTrackableTime, 5)
{
}

void TTopicWorkloadStats::AddEvent(const WriterEvent& event)
{
    WriteMessages++;
    WriteBytes += event.MessageSize;
    WriteTimeHist.RecordValue(Min(event.WriteTime, HighestTrackableTime));
    InflightMessagesHist.RecordValue(Min(event.InflightMessages, HighestTrackableMessageCount));
}

void TTopicWorkloadStats::AddEvent(const ReaderEvent& event)
{
    ReadMessages++;
    ReadBytes += event.MessageSize;
    FullTimeHist.RecordValue(Min(event.FullTime, HighestTrackableTime));
}

void TTopicWorkloadStats::AddEvent(const LagEvent& event)
{
    LagMessagesHist.RecordValue(Min(event.LagMessages, HighestTrackableMessageCount));
    LagTimeHist.RecordValue(Min(event.LagTime, HighestTrackableTime));
}
