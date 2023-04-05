#include "topic_workload_stats.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStats::TTopicWorkloadStats()
    : WriteBytes(0)
    , WriteMessages(0)
    , WriteTimeHist(60000, 2)
    , InflightMessages(0)
    , LagMessages(0)
    , LagTimeHist(60000, 2)
    , ReadBytes(0)
    , ReadMessages(0)
    , FullTimeHist(60000, 5)
{
}

void TTopicWorkloadStats::AddWriterEvent(ui64 messageSize, ui64 writeTime, ui64 inflightMessages)
{
    WriteMessages++;
    WriteBytes += messageSize;
    WriteTimeHist.RecordValue(writeTime);
    InflightMessages = Max(InflightMessages, inflightMessages);
}

void TTopicWorkloadStats::AddReaderEvent(ui64 messageSize, ui64 fullTime)
{
    ReadMessages++;
    ReadBytes += messageSize;
    FullTimeHist.RecordValue(fullTime);
}

void TTopicWorkloadStats::AddLagEvent(ui64 lagMessages, ui64 lagTime)
{
    LagMessages = Max(LagMessages, lagMessages);
    LagTimeHist.RecordValue(lagTime);
}